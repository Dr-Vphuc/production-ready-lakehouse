"""
DAG lakehouse_maintenance: dọn dẹp lakehouse mỗi giờ trong lúc chạy realtime.

    compact_bronze -> nessie_gc

1. compact_bronze: gộp các file nhỏ mà job streaming ghi vào bronze.events (mỗi phút vài file)
   thành file lớn, bằng thủ tục Iceberg rewrite_data_files gọi qua dbt run-operation.
2. nessie_gc: xoá khỏi MinIO các file mà không phiên bản nào trong 1 giờ gần nhất còn dùng:
   bản cũ của silver/gold sau mỗi lần dbt ghi lại, các file nhỏ đã được gộp, ...
   Sau khi dọn thì không time travel về quá 1 giờ trước được nữa.
"""

import os
from datetime import timedelta

import pendulum

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Thư mục dbt project bên trong airflow-worker (mount từ ./dbt).
DBT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="lakehouse_maintenance",
    # Phút thứ 5 mỗi giờ, lệch khỏi các mốc 0/15/30/45 của lakehouse_intraday.
    schedule="5 * * * *",
    start_date=pendulum.datetime(2026, 10, 15, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["lakehouse", "realtime"],
) as dag:

    # Bước 1: chạy qua spark-thrift giống dbt nên dùng chung pool dbt.
    compact_bronze = BashOperator(
        task_id="compact_bronze",
        bash_command="dbt run-operation compact_table --args '{table_name: bronze.events}'",
        cwd=DBT_DIR,
        pool="dbt",
    )

    # Bước 2: công cụ GC của Nessie, chạy trong container riêng giống ingest_bronze.
    nessie_gc = DockerOperator(
        task_id="nessie_gc",
        image="ghcr.io/projectnessie/nessie-gc:0.108.8",
        command=[
            "gc",
            # Lưu danh sách file còn dùng trong RAM, không cần database riêng
            "--inmemory",
            "--uri", "http://nessie:19120/api/v2",
            # Mọi phiên bản dữ liệu trong 1 giờ gần nhất được giữ lại
            "--default-cutoff", "PT1H",
            # Không xoá file mới hơn 1 giờ: bảo vệ file mà streaming hoặc dbt
            # vừa ghi nhưng chưa kịp commit
            "--max-file-modification",
            "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ') }}",
            "--iceberg", "s3.endpoint=http://minio:9000,s3.path-style-access=true",
        ],
        environment={
            "AWS_REGION": "us-east-1",
            "JAVA_TOOL_OPTIONS": "-Xmx512m",
        },
        # Tài khoản MinIO: không lưu vào DB của Airflow và không hiện trên UI.
        private_environment={
            "AWS_ACCESS_KEY_ID": os.environ["MINIO_ROOT_USER"],
            "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_ROOT_PASSWORD"],
        },
        network_mode="realtime-lakehouse_default",
        mount_tmp_dir=False,
        auto_remove="success",
    )

    compact_bronze >> nessie_gc
