"""
==============================================================================
MODULE 02: SENSORS TRONG AIRFLOW (EVENT-DRIVEN PATTERNS)
==============================================================================
Mục tiêu bài học:
1. Hiểu bản chất Sensor: Chờ đợi sự kiện bên ngoài trước khi thực thi tiếp.
2. Phân biệt chế độ `poke` (chiếm dụng worker slot) vs `reschedule` (giải phóng slot worker - khuyên dùng).
3. Sử dụng FileSensor / PythonSensor và cấu hình timeout, poke_interval.
"""

from datetime import datetime
import os
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator

LANDING_FILE_PATH = "/tmp/landing_data.csv"

def check_external_system_status(**kwargs):
    """Giả lập kiểm tra trạng thái của API hoặc Microservice bên ngoài."""
    print("Đang kiểm tra trạng thái Microservice...")
    # Giả lập trả về True (hệ thống đã sẵn sàng)
    return True

def process_landed_file(**kwargs):
    print("Dữ liệu đầu vào đã sẵn sàng! Tiến hành đọc và xử lý...")

with DAG(
    dag_id="02_sensors_demo",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Kích hoạt thủ công hoặc qua Trigger
    catchup=False,
    tags=["module_02", "sensors", "event_driven"],
) as dag:

    # 1. PythonSensor với mode='reschedule' giúp tiết kiệm tài nguyên worker
    wait_for_api_ready = PythonSensor(
        task_id="wait_for_api_ready",
        python_callable=check_external_system_status,
        poke_interval=10,       # Thử lại sau mỗi 10 giây
        timeout=300,            # Hết thời gian chờ sau 5 phút (tránh treo vĩnh viễn)
        mode="reschedule",      # GIẢI PHÓNG WORKER SLOT trong thời gian chờ
    )

    # 2. Xử lý sau khi sensor hoàn tất điều kiện
    process_data = PythonOperator(
        task_id="process_data_after_sensor",
        python_callable=process_landed_file,
    )

    wait_for_api_ready >> process_data
