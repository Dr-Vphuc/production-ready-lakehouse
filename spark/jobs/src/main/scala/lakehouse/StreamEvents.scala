package lakehouse

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object StreamEvents {

  private val Topic         = "ecommerce.events"
  private val Bootstrap     = "kafka:9092"
  private val Table         = "bronze.events"
  private val RejectedTable = "bronze.events_rejected"
  private val CheckpointDir = "/opt/spark/checkpoints"
  private val TriggerEvery  = "1 minute"

  // Producer gửi mỗi sự kiện là một JSON, mọi giá trị ở dạng chuỗi giống cột CSV
  private val RawSchema = StructType(Seq(
    "event_time", "event_type", "product_id", "category_id", "category_code",
    "brand", "price", "user_id", "user_session"
  ).map(StructField(_, StringType)))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("stream-events").getOrCreate()
    // Ép kiểu lỗi trả về NULL thay vì ném lỗi: một tin hỏng không được làm dừng cả stream
    spark.conf.set("spark.sql.ansi.enabled", "false")

    // bronze.events do job batch tạo, có partition theo tháng. Nếu để toTable tự tạo bảng
    // thì bảng sẽ không có partition, nên bắt buộc bảng phải có sẵn.
    require(spark.catalog.tableExists(Table), s"$Table chưa tồn tại, hãy chạy job batch trước")

    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $RejectedTable (
         |  raw_value       STRING,
         |  reason          STRING,
         |  kafka_partition INT,
         |  kafka_offset    BIGINT,
         |  kafka_timestamp TIMESTAMP,
         |  _ingested_at    TIMESTAMP
         |) USING iceberg""".stripMargin)

    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Bootstrap)
      .option("subscribe", Topic)
      .option("startingOffsets", "earliest")     // chỉ dùng ở lần chạy đầu; sau đó đọc tiếp theo checkpoint
      .option("maxOffsetsPerTrigger", "200000")  // giới hạn một micro-batch khi phải đọc bù
      .load()

    val events = parse(kafka)

    // Hai query đọc cùng topic, mỗi query có checkpoint riêng nên đều exactly-once
    toBronze(events).writeStream
      .queryName("events-to-bronze")
      .format("iceberg")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(TriggerEvery))
      .option("fanout-enabled", "true")  // một micro-batch có thể chạm 2 partition tháng
      .option("checkpointLocation", s"$CheckpointDir/events")
      .toTable(Table)

    toRejected(events).writeStream
      .queryName("events-to-rejected")
      .format("iceberg")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(TriggerEvery))
      .option("checkpointLocation", s"$CheckpointDir/events_rejected")
      .toTable(RejectedTable)

    // Một query lỗi thì cả job dừng; Docker khởi động lại container, hai query đọc tiếp từ checkpoint
    spark.streams.awaitAnyTermination()
  }

  // Tách JSON, ép kiểu giống job batch, và ghi lý do nếu tin không hợp lệ
  def parse(kafka: DataFrame): DataFrame = {
    val r = from_json(col("value").cast(StringType), RawSchema)

    def failedCast(raw: Column, typed: Column): Column = raw.isNotNull && typed.isNull

    kafka
      .select(
        col("value").cast(StringType).as("raw_value"),
        col("topic"),
        col("partition").as("kafka_partition"),
        col("offset").as("kafka_offset"),
        col("timestamp").as("kafka_timestamp"),
        r.as("r"))
      .select(
        col("*"),
        col("r.event_time").cast(TimestampType).as("event_time"),
        col("r.event_type").as("event_type"),
        col("r.product_id").cast(LongType).as("product_id"),
        col("r.category_id").cast(LongType).as("category_id"),
        col("r.category_code").as("category_code"),
        col("r.brand").as("brand"),
        col("r.price").cast(DecimalType(10, 2)).as("price"),
        col("r.user_id").cast(LongType).as("user_id"),
        col("r.user_session").as("user_session"))
      .withColumn("reason", concat_ws(",",
        // Các cột bắt buộc (silver có test not_null), và cột partition event_time
        when(col("event_time").isNull, lit("event_time")),
        when(col("event_type").isNull, lit("event_type")),
        when(col("product_id").isNull, lit("product_id")),
        when(col("user_id").isNull, lit("user_id")),
        // Cột không bắt buộc: chỉ lỗi khi có giá trị mà ép kiểu thất bại
        when(failedCast(col("r.category_id"), col("category_id")), lit("category_id")),
        when(failedCast(col("r.price"), col("price")), lit("price"))))
  }

  // Tin hợp lệ: đúng schema của bronze.events. _source_file ghi vị trí tin trong Kafka
  def toBronze(events: DataFrame): DataFrame =
    events
      .where(col("reason") === "")
      .select(
        col("event_time"), col("event_type"), col("product_id"), col("category_id"),
        col("category_code"), col("brand"), col("price"), col("user_id"), col("user_session"),
        concat(lit("kafka://"), col("topic"), lit("/"), col("kafka_partition"), lit("/"), col("kafka_offset"))
          .as("_source_file"),
        current_timestamp().as("_ingested_at"))

  // Tin không hợp lệ: giữ nguyên nội dung gốc để xem lại
  def toRejected(events: DataFrame): DataFrame =
    events
      .where(col("reason") =!= "")
      .select(
        col("raw_value"), col("reason"), col("kafka_partition"), col("kafka_offset"),
        col("kafka_timestamp"), current_timestamp().as("_ingested_at"))
}
