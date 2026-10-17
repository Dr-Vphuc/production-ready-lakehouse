-- Nhánh realtime phục vụ BI: ClickHouse tự đọc topic Kafka, dữ liệu lên dashboard sau vài giây.
-- Chạy bởi service clickhouse-init (clickhouse-client --queries-file). Mọi lệnh đều IF NOT EXISTS
-- nên chạy lại không sao, nhưng cũng vì vậy mà sửa file này KHÔNG cập nhật object đã có
-- (cách đổi xem mục "Realtime trên ClickHouse" trong README).
--
--   ecommerce.events ──► rt.events_queue (Kafka) ──MV──► rt.events_parsed (Null: ép kiểu + reason)
--                                                          ├─MV (reason = '')  ──► rt.events
--                                                          └─MV (reason != '') ──► rt.events_rejected
--
-- Nhánh này không thay Spark: bronze vẫn do StreamEvents ghi. Hai bên đọc cùng topic nhưng độc lập,
-- ClickHouse giữ offset trong consumer group clickhouse-rt của Kafka, Spark giữ trong checkpoint.
--
-- Khác Iceberg (database lakehouse), đây là bảng riêng của ClickHouse, nằm trong volume clickhouse-data.
-- DROP trong database rt chỉ xóa bản sao này, không đụng tới lakehouse.

CREATE DATABASE IF NOT EXISTS rt;

-- Bảng đích: sự kiện hợp lệ, cùng cột nghiệp vụ với bronze.events.
--   kafka_timestamp : lúc producer gửi tin (timestamp của tin Kafka)
--   ingested_at     : lúc ClickHouse ghi xong, nên ingested_at - kafka_timestamp là độ trễ của nhánh này
-- Không đặt TTL: event_time là năm 2020, TTL tính theo now() sẽ xóa sạch dữ liệu.
-- Giữ nguyên trạng như bronze, kể cả dòng trùng. Truy vấn phục vụ tự khử trùng (xem dataset rt_events_today).
CREATE TABLE IF NOT EXISTS rt.events
(
    event_time      DateTime('UTC'),
    event_type      LowCardinality(String),
    product_id      Int64,
    category_id     Nullable(Int64),
    category_code   LowCardinality(Nullable(String)),
    brand           LowCardinality(Nullable(String)),
    price           Nullable(Decimal(10, 2)),
    user_id         Int64,
    user_session    Nullable(String),
    kafka_partition UInt32,
    kafka_offset    UInt64,
    kafka_timestamp Nullable(DateTime64(3, 'UTC')),
    ingested_at     DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY event_time;

-- Tin không hợp lệ, cùng cột với bronze.events_rejected
CREATE TABLE IF NOT EXISTS rt.events_rejected
(
    raw_value       String,
    reason          LowCardinality(String),
    kafka_partition UInt32,
    kafka_offset    UInt64,
    kafka_timestamp Nullable(DateTime64(3, 'UTC')),
    ingested_at     DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY ingested_at;

-- Engine Null không lưu gì. Bảng này chỉ là điểm trung chuyển: logic ép kiểu nằm một lần ở MV phía trên,
-- hai MV phía dưới tách dòng tốt/dòng lỗi (giống parse -> toBronze / toRejected trong StreamEvents.scala).
CREATE TABLE IF NOT EXISTS rt.events_parsed
(
    event_time      Nullable(DateTime('UTC')),
    event_type      Nullable(String),
    product_id      Nullable(Int64),
    category_id     Nullable(Int64),
    category_code   Nullable(String),
    brand           Nullable(String),
    price           Nullable(Decimal(10, 2)),
    user_id         Nullable(Int64),
    user_session    Nullable(String),
    reason          String,
    raw_value       String,
    kafka_partition UInt32,
    kafka_offset    UInt64,
    kafka_timestamp Nullable(DateTime64(3, 'UTC')),
    ingested_at     DateTime64(3, 'UTC')
)
ENGINE = Null;

-- Hai MV phía dưới phải có trước MV đọc Kafka: MV đó vừa tạo là bắt đầu đọc và commit offset,
-- nếu lúc ấy chưa có MV phía dưới thì tin đi vào bảng Null rồi mất.

-- 4 cột bắt buộc chắc chắn có giá trị khi reason = '', nên assumeNotNull để vừa kiểu không Nullable của bảng đích
CREATE MATERIALIZED VIEW IF NOT EXISTS rt.events_mv TO rt.events AS
SELECT
    assumeNotNull(event_time) AS event_time,
    assumeNotNull(event_type) AS event_type,
    assumeNotNull(product_id) AS product_id,
    category_id,
    category_code,
    brand,
    price,
    assumeNotNull(user_id) AS user_id,
    user_session,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    ingested_at
FROM rt.events_parsed
WHERE reason = '';

CREATE MATERIALIZED VIEW IF NOT EXISTS rt.events_rejected_mv TO rt.events_rejected AS
SELECT raw_value, reason, kafka_partition, kafka_offset, kafka_timestamp, ingested_at
FROM rt.events_parsed
WHERE reason != '';

-- Kafka engine: không lưu dữ liệu, chỉ là "đầu đọc" topic. Chỉ đọc khi có MV gắn vào,
-- và offset được commit sau khi các MV ghi xong (at-least-once: ClickHouse chết giữa chừng thì có thể trùng vài dòng).
-- Mọi cột là chuỗi vì producer gửi mọi giá trị ở dạng chuỗi (giống RawSchema của StreamEvents).
--   kafka_group_name        : consumer group riêng; group mới đọc từ đầu topic
--   kafka_num_consumers     : 1 consumer đọc cả 3 partition, đủ cho máy local
--   kafka_handle_error_mode : 'stream' = JSON hỏng không làm kẹt luồng, lỗi ra ở cột ảo _error, _raw_message
--   kafka_flush_interval_ms : cứ 2 giây ghi một lần dù block chưa đầy, độ trễ chỉ vài giây (mặc định 7,5 giây)
--   kafka_max_block_size    : tối đa 100.000 tin mỗi lần ghi, giữ RAM thấp khi đọc bù cả tháng
-- Không SELECT thẳng bảng này: phiên SELECT sẽ chen vào consumer group và lấy mất tin của MV
-- (user superset đã bị REVOKE quyền đọc bảng này ở clickhouse-init).
CREATE TABLE IF NOT EXISTS rt.events_queue
(
    event_time    Nullable(String),
    event_type    Nullable(String),
    product_id    Nullable(String),
    category_id   Nullable(String),
    category_code Nullable(String),
    brand         Nullable(String),
    price         Nullable(String),
    user_id       Nullable(String),
    user_session  Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'ecommerce.events',
         kafka_group_name = 'clickhouse-rt',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_handle_error_mode = 'stream',
         kafka_flush_interval_ms = 2000,
         kafka_max_block_size = 100000;

-- Ép kiểu và ghi lý do, cùng luật với StreamEvents.parse.
-- q.<cột> là chuỗi gốc từ Kafka; tên không có q. là alias đã ép kiểu ở ngay SELECT này.
-- Chỉ dùng hàm ...OrNull: một lỗi ném ra trong MV sẽ làm Kafka engine đọc lại mãi đúng lô tin đó.
-- Tạo sau cùng: từ đây ClickHouse bắt đầu đọc topic. Muốn tạm dừng: DETACH TABLE rt.events_parse_mv.
CREATE MATERIALIZED VIEW IF NOT EXISTS rt.events_parse_mv TO rt.events_parsed AS
WITH accurateCastOrNull(q.price, 'Decimal(10, 2)') AS price_cast
SELECT
    parseDateTimeBestEffortOrNull(q.event_time, 'UTC') AS event_time,
    q.event_type AS event_type,
    accurateCastOrNull(q.product_id, 'Int64') AS product_id,
    accurateCastOrNull(q.category_id, 'Int64') AS category_id,
    q.category_code AS category_code,
    q.brand AS brand,
    -- Spark trả NULL khi giá vượt Decimal(10, 2) (từ 10^8 trở lên); ClickHouse không tự kiểm nên kiểm tay
    if(abs(price_cast) < 100000000, price_cast, NULL) AS price,
    accurateCastOrNull(q.user_id, 'Int64') AS user_id,
    q.user_session AS user_session,
    if(_error != '',
       -- JSON hỏng: mọi cột đều NULL, ghi rõ lý do thay vì liệt kê cả 4 cột bắt buộc
       'json',
       arrayStringConcat(arrayFilter(x -> x != '', [
           -- Các cột bắt buộc (silver có test not_null), và cột partition event_time
           if(event_time IS NULL, 'event_time', ''),
           if(event_type IS NULL, 'event_type', ''),
           if(product_id IS NULL, 'product_id', ''),
           if(user_id IS NULL, 'user_id', ''),
           -- Cột không bắt buộc: chỉ lỗi khi có giá trị mà ép kiểu thất bại
           if(q.category_id IS NOT NULL AND category_id IS NULL, 'category_id', ''),
           if(q.price IS NOT NULL AND price IS NULL, 'price', '')]), ',')) AS reason,
    -- _raw_message chỉ có nội dung khi JSON hỏng; tin đọc được thì dựng lại JSON từ các chuỗi gốc
    if(_error != '',
       _raw_message,
       toJSONString(map(
           'event_time', q.event_time, 'event_type', q.event_type, 'product_id', q.product_id,
           'category_id', q.category_id, 'category_code', q.category_code, 'brand', q.brand,
           'price', q.price, 'user_id', q.user_id, 'user_session', q.user_session))) AS raw_value,
    _partition AS kafka_partition,
    _offset AS kafka_offset,
    _timestamp_ms AS kafka_timestamp,
    now64(3) AS ingested_at
FROM rt.events_queue AS q;
