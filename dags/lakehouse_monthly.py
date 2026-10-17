"""
DAG lakehouse_monthly: chạy pipeline batch theo từng tháng dữ liệu.

Mỗi run xử lý đúng một tháng (lấy từ data_interval_start) qua 3 bước:
    ingest_bronze -> dbt_silver -> dbt_gold

1. ingest_bronze: chạy job Spark (Scala) đọc file CSV của tháng trong archive/
   rồi ghi đè partition của tháng đó trong bảng bronze.events.
2. dbt_silver: dbt làm sạch, bỏ dòng trùng rồi ghi đè partition của tháng đó
   trong silver.events, sau đó chạy test của model.
3. dbt_gold: dbt tính lại toàn bộ các bảng gold (fct_orders, fct_sessions,
   dim_products, daily_kpis) rồi chạy test.

Chạy lại bước nào cũng cho cùng kết quả, nên retry an toàn.

Batch chỉ phụ trách các tháng đã đóng (10/2019 -> 01/2020). Tháng 02/2020 do job streaming
nạp từ Kafka, nên DAG này không được ghi vào partition tháng đó.
"""

import os
from datetime import timedelta

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, CronDataIntervalTimetable
from docker.types import Mount

# Đường dẫn project trên máy host (lấy từ .env). Container do DockerOperator tạo
# chạy song song với airflow-worker chứ không nằm bên trong, nên bind mount phải
# dùng đường dẫn của host.
HOST_PROJECT_DIR = os.environ["HOST_PROJECT_DIR"]
# Thư mục dbt project bên trong airflow-worker (mount từ ./dbt).
DBT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="lakehouse_monthly",
    # Mỗi tháng một run. Run của tháng 10 có khoảng dữ liệu [01/10, 01/11)
    # và chỉ chạy sau khi tháng 10 kết thúc.
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),
    # Batch phụ trách 10/2019 -> 01/2020, tức 4 run. Tháng 02/2020 thuộc về streaming.
    start_date=pendulum.datetime(2019, 10, 1, tz="UTC"),
    end_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    # Chạy bù các tháng trong quá khứ (Airflow 3 mặc định tắt).
    catchup=True,
    # Chạy lần lượt từng tháng: Spark có ít core, và mỗi run ghi đè toàn bộ gold.
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        # Task chạy quá 30 phút sẽ bị dừng, tránh treo mãi.
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["lakehouse"],
) as dag:

    # Bước 1: tạo container từ image my-spark-jobs rồi spark-submit job ingest
    # lên Spark cluster, giống service spark-ingest trong docker-compose.
    ingest_bronze = DockerOperator(
        task_id="ingest_bronze",
        image="my-spark-jobs:latest",
        entrypoint=[
            "/opt/spark/bin/spark-submit",
            "--class", "lakehouse.IngestEvents",
            "--name", "ingest-events",
            "--master", "spark://spark-master:7077",
            "--conf", "spark.executor.cores=2",
            "--conf", "spark.cores.max=2",
            "/opt/spark/apps/lakehouse-jobs.jar",
        ],
        # Tham số tháng cho job, ví dụ "2019-Oct" (trùng tên file CSV).
        command=["{{ data_interval_start.strftime('%Y-%b') }}"],
        environment={"AWS_REGION": "us-east-1"},
        # Tài khoản MinIO: không lưu vào DB của Airflow và không hiện trên UI.
        private_environment={
            "AWS_ACCESS_KEY_ID": os.environ["MINIO_ROOT_USER"],
            "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_ROOT_PASSWORD"],
        },
        mounts=[
            # Cấu hình Spark: catalog Iceberg trỏ tới Nessie, lưu file trên MinIO.
            Mount(
                source=f"{HOST_PROJECT_DIR}/spark/conf/spark-defaults.conf",
                target="/opt/spark/conf/spark-defaults.conf",
                type="bind",
                read_only=True,
            ),
            # Thư mục chứa các file CSV nguồn.
            Mount(
                source=f"{HOST_PROJECT_DIR}/archive",
                target="/data/archive",
                type="bind",
                read_only=True,
            ),
        ],
        # Cùng network với spark-master, nessie và minio.
        network_mode="realtime-lakehouse_default",
        # Không mount /tmp của worker vì đường dẫn đó không có trên host.
        mount_tmp_dir=False,
        # Container được xoá khi chạy thành công; khi lỗi thì giữ lại để xem docker logs.
        auto_remove="success",
    )

    # Bước 2: dbt build model silver events cho riêng tháng này
    # (ghi đè partition của tháng) rồi chạy test của model.
    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command='dbt build --select events --vars "{month: $MONTH}"',
        # Tháng dạng "2019-10". Truyền qua biến môi trường để dấu ngoặc của
        # Jinja và YAML không dính vào nhau.
        env={"MONTH": "{{ data_interval_start.strftime('%Y-%m') }}"},
        # Giữ lại các biến môi trường sẵn có (PATH, ...) để vẫn gọi được lệnh dbt.
        append_env=True,
        cwd=DBT_DIR,
        # Pool 1 slot dùng chung với các DAG khác: không có hai lệnh dbt chạy cùng lúc.
        pool="dbt",
    )

    # Bước 3: dbt build lại toàn bộ model gold rồi chạy test
    # (kể cả test đối soát revenue).
    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command="dbt build --select path:models/gold",
        cwd=DBT_DIR,
        pool="dbt",
    )

    # Thứ tự chạy: bronze -> silver -> gold.
    ingest_bronze >> dbt_silver >> dbt_gold
