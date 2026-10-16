"""
Giả lập ứng dụng thương mại điện tử: phát lại một file CSV trong archive/ vào Kafka.

- Mỗi dòng CSV thành một tin JSON, giữ nguyên giá trị dạng chuỗi (ô trống thành null).
- Key của tin là user_session, nên các sự kiện của cùng một phiên vào cùng một partition
  và giữ đúng thứ tự.
- Nhịp gửi bám theo event_time, tua nhanh --speed lần. Ví dụ --speed 288: 1 ngày dữ liệu
  phát trong 5 phút. --speed 0: gửi nhanh hết mức, không chờ.
- Dừng bằng Ctrl+C: producer gửi nốt các tin đang chờ rồi in ra số tin đã giao thành công.
  Chạy tiếp từ chỗ đó bằng --skip <số tin đã giao>.
"""

import argparse
import csv
import json
import signal
import sys
import time
from datetime import datetime

from confluent_kafka import KafkaException, Producer

FIELDS = [
    "event_time", "event_type", "product_id", "category_id", "category_code",
    "brand", "price", "user_id", "user_session",
]
REPORT_EVERY_SECONDS = 10


def parse_args():
    parser = argparse.ArgumentParser(description="Phát lại file CSV sự kiện vào Kafka")
    parser.add_argument("--month", required=True, help="Tên file trong archive/, ví dụ 2020-Feb")
    parser.add_argument("--speed", type=float, default=288, help="Hệ số tua nhanh; 0 = không chờ")
    parser.add_argument("--skip", type=int, default=0, help="Bỏ qua N dòng đầu (để chạy tiếp)")
    parser.add_argument("--limit", type=int, default=0, help="Chỉ gửi N dòng; 0 = cả file")
    parser.add_argument("--topic", default="ecommerce.events")
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--archive", default="/data/archive")
    return parser.parse_args()


def event_seconds(value):
    # event_time dạng "2020-02-01 00:00:01 UTC"; chỉ cần khoảng cách giữa các sự kiện
    return datetime.fromisoformat(value[:19]).timestamp()


def stop(signum, frame):
    raise KeyboardInterrupt


def check_topic(producer, args):
    # Kiểm tra sớm: Kafka phải chạy và topic phải dùng được.
    # Topic vừa được tạo có thể cần vài giây mới có leader, nên thử lại vài lần.
    for _ in range(5):
        try:
            metadata = producer.list_topics(args.topic, timeout=15)
        except KafkaException as exc:
            sys.exit(f"Không kết nối được Kafka ở {args.bootstrap}: {exc}")
        error = metadata.topics[args.topic].error
        if error is None:
            return
        time.sleep(2)
    sys.exit(f"Topic {args.topic} không dùng được: {error}")


def main():
    args = parse_args()

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        # Producer tự gắn số thứ tự cho tin, broker bỏ tin trùng khi producer gửi lại
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 50,
    })

    check_topic(producer, args)

    stats = {"delivered": 0, "failed": 0}

    def on_delivery(err, msg):
        if err is None:
            stats["delivered"] += 1
        else:
            stats["failed"] += 1
            if stats["failed"] <= 10:
                print(f"Gửi lỗi: {err}", file=sys.stderr)

    # docker compose stop gửi SIGTERM: xử lý giống Ctrl+C
    signal.signal(signal.SIGTERM, stop)

    path = f"{args.archive}/{args.month}.csv"
    print(f"Phát lại {path} -> {args.topic} (speed={args.speed}, skip={args.skip}, limit={args.limit})")

    sent = 0
    last_event_time = None
    start_wall = None
    start_event = None
    last_report = time.monotonic()

    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames != FIELDS:
                sys.exit(f"Header CSV không khớp: {reader.fieldnames}")

            for line_no, row in enumerate(reader):
                if line_no < args.skip:
                    continue
                if args.limit and sent >= args.limit:
                    break

                # Chờ tới đúng thời điểm của sự kiện trên "đồng hồ tua nhanh"
                if args.speed > 0:
                    ts = event_seconds(row["event_time"])
                    if start_wall is None:
                        start_wall, start_event = time.monotonic(), ts
                    delay = start_wall + (ts - start_event) / args.speed - time.monotonic()
                    if delay > 0:
                        producer.poll(0)
                        time.sleep(delay)

                value = json.dumps({k: (row[k] if row[k] != "" else None) for k in FIELDS})
                key = row["user_session"] or None

                while True:
                    try:
                        producer.produce(args.topic, key=key, value=value, on_delivery=on_delivery)
                        break
                    except BufferError:
                        # Hàng đợi nội bộ đầy: chờ broker xác nhận bớt rồi gửi lại
                        producer.poll(1)

                producer.poll(0)
                sent += 1
                last_event_time = row["event_time"]

                now = time.monotonic()
                if now - last_report >= REPORT_EVERY_SECONDS:
                    print(f"đã gửi {sent:,} tin, tới event_time {last_event_time}")
                    last_report = now
    except KeyboardInterrupt:
        print("Dừng theo yêu cầu, đang gửi nốt các tin còn chờ...")
    except OSError as exc:
        # Ví dụ máy sleep làm mất thư mục mount: vẫn gửi nốt và in điểm chạy tiếp
        print(f"Lỗi đọc file ({exc}), đang gửi nốt các tin còn chờ...", file=sys.stderr)


    remaining = producer.flush(60)
    delivered = stats["delivered"]
    print(f"Xong: đã giao {delivered:,} tin (lỗi {stats['failed']:,}, còn kẹt {remaining:,}), "
          f"event_time cuối {last_event_time}")

    if stats["failed"] or remaining:
        # Có tin lỗi thì không biết chắc tin nào đã vào Kafka, chạy tiếp bằng --skip có thể sót hoặc trùng
        sys.exit("Có tin không giao được, không xác định được điểm chạy tiếp")
    print(f"Muốn chạy tiếp từ đây: --skip {args.skip + delivered}")


if __name__ == "__main__":
    main()
