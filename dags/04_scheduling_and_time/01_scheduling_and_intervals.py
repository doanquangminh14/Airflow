"""
==============================================================================
MODULE 04: LẬP LỊCH & KHOẢNG THỜI GIAN (SCHEDULING & DATA INTERVALS)
==============================================================================
Mục tiêu bài học:
1. Hiểu khái niệm quan trọng nhất của Airflow: Logical Date (Execution Date) vs Run Date.
2. Hiểu vì sao DAG chạy lúc `data_interval_end` (sau khi khoảng thời gian dữ liệu kết thúc).
3. Sử dụng các Jinja Template Variables: `data_interval_start`, `data_interval_end`, `ds`, `ts`.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def inspect_data_intervals(**context):
    """
    In chi tiết các biến thời gian của một DagRun.
    """
    logical_date = context.get("logical_date")
    data_interval_start = context.get("data_interval_start")
    data_interval_end = context.get("data_interval_end")
    ds = context.get("ds")  # Định dạng YYYY-MM-DD
    
    print("================ KHOẢNG THỜI GIAN DỮ LIỆU ================")
    print(f"1. Logical Date (Execution Date) : {logical_date}")
    print(f"2. Data Interval Start           : {data_interval_start}")
    print(f"3. Data Interval End (Trigger)   : {data_interval_end}")
    print(f"4. Ngày xử lý (ds)               : {ds}")
    print("==========================================================")
    print("QUY TẮC VÀNG: Airflow xử lý dữ liệu của khoảng [start -> end],")
    print("và chỉ bắt đầu chạy khi thời gian 'end' đã trôi qua!")

with DAG(
    dag_id="01_scheduling_and_intervals",
    start_date=datetime(2024, 1, 1),
    # Chạy mỗi giờ vào phút thứ 0. (Ví dụ khoảng 01:00 -> 02:00 sẽ chạy lúc 02:00)
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["module_04", "scheduling", "intervals", "cron"],
) as dag:

    explain_time_task = PythonOperator(
        task_id="inspect_time_variables",
        python_callable=inspect_data_intervals,
    )
