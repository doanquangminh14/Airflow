"""
==============================================================================
MODULE 04: CATCHUP, BACKFILL & PHỤ THUỘC QUÁ KHỨ (DEPENDS_ON_PAST)
==============================================================================
Mục tiêu bài học:
1. Ý nghĩa tham số `catchup=True` vs `catchup=False`.
2. Tham số `max_active_runs`: giới hạn số DagRun chạy song song cùng lúc để tránh làm sập DB.
3. Tham số `depends_on_past=True`: task của hôm nay chỉ chạy nếu task của hôm qua thành công.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def sequential_daily_process(**context):
    ds = context["ds"]
    print(f"Đang xử lý dữ liệu cho ngày: {ds}")

with DAG(
    dag_id="02_backfill_and_catchup",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    # catchup=False: Chỉ chạy lần gần nhất, không tự động sinh hàng trăm DagRun cũ
    catchup=False,
    # max_active_runs=1: Đảm bảo chỉ 1 ngày được xử lý tại 1 thời điểm
    max_active_runs=1,
    tags=["module_04", "catchup", "backfill", "depends_on_past"],
) as dag:

    daily_task = PythonOperator(
        task_id="process_historical_partition",
        python_callable=sequential_daily_process,
        # depends_on_past=True: Bảo vệ tính toàn vẹn tuần tự của chuỗi thời gian
        depends_on_past=False,
    )
