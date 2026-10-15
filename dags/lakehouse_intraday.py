"""
DAG lakehouse_intraday: làm mới silver và gold mỗi 15 phút trong lúc streaming nạp bronze.

    dbt_silver -> dbt_gold

1. dbt_silver: không truyền month. Model silver tự tìm các tháng có dòng mới trong bronze
   kể từ lần build trước, rồi build lại trọn các tháng đó và chạy test.
2. dbt_gold: tính lại toàn bộ các bảng gold rồi chạy test.

Chỉ nên bật DAG này khi đang phát lại dữ liệu: mỗi lần chạy ghi lại khoảng 300 MiB,
bản cũ chỉ được xoá khi lakehouse_maintenance chạy Nessie GC.
"""

from datetime import timedelta

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Thư mục dbt project bên trong airflow-worker (mount từ ./dbt).
DBT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="lakehouse_intraday",
    # Chuỗi cron thường: run không gắn với khoảng dữ liệu nào, chỉ cần chạy đều đặn.
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 10, 15, tz="UTC"),
    # Không chạy bù: lần làm mới gần nhất đã bao gồm mọi dữ liệu có trước nó.
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        # Task chạy quá 20 phút sẽ bị dừng, tránh treo mãi.
        "execution_timeout": timedelta(minutes=20),
        # Pool 1 slot dùng chung với lakehouse_monthly và lakehouse_maintenance.
        "pool": "dbt",
    },
    tags=["lakehouse", "realtime"],
) as dag:

    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command="dbt build --select events",
        cwd=DBT_DIR,
    )

    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command="dbt build --select path:models/gold",
        cwd=DBT_DIR,
    )

    dbt_silver >> dbt_gold
