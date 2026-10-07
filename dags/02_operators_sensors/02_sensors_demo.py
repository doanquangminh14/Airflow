"""
==============================================================================
MODULE 02: SENSORS TRONG AIRFLOW (EVENT-DRIVEN PATTERNS)
==============================================================================
Mục tiêu bài học:
1. Hiểu bản chất Sensor: Chờ đợi sự kiện/tài nguyên bên ngoài trước khi thực thi tiếp.
2. Phân biệt triệt để chế độ `poke` (chiếm slot worker) vs `reschedule` (giải phóng slot worker).
3. Cấu hình các tham số bảo vệ tài nguyên: poke_interval, timeout, exponential_backoff, soft_fail.
4. Triển khai PythonSensor với logic kiểm tra điều kiện an toàn.
==============================================================================
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

logger = logging.getLogger("airflow.task")

DAG_DOC_MD = """
### ⏳ DAG: Sensors Event-Driven Patterns
Demo cơ chế lắng nghe sự kiện của Airflow Sensor:
- **`mode="reschedule"`**: Sau mỗi lần thăm dò không thỏa điều kiện, sensor trả slot worker lại cho cluster.
- **`poke_interval=15`**: Tần suất thăm dò mỗi 15 giây.
- **`timeout=300`**: Giới hạn tối đa 5 phút để tránh tắc nghẽn luồng vô tận.
- **`soft_fail=True`**: Nếu timeout, task chuyển sang trạng thái SKIPPED thay vì FAILED nếu mong muốn.
"""


def check_external_system_ready(**context: Any) -> bool:
    """Giả lập hàm kiểm tra trạng thái sức khỏe của Microservice / Data Source bên ngoài.

    Returns:
        bool: True nếu dữ liệu hoặc API đã sẵn sàng để xử lý, ngược lại False.
    """
    ti = context.get("ti")
    logger.info("Đang thăm dò (probing) trạng thái Microservice...")

    # Giả lập thành công ngay lập tức để demo chạy thông suốt
    service_status_online = True
    if service_status_online:
        logger.info("Microservice đã ONLINE! Cho phép pipeline tiếp tục thực thi.")
        return True

    logger.warning("Microservice chưa phản hồi. Tiếp tục chờ lần thử tiếp theo...")
    return False


def process_landed_data(**context: Any) -> None:
    """Xử lý dữ liệu sau khi sensor xác nhận nguồn dữ liệu đã sẵn sàng."""
    logical_date = context.get("ds", "N/A")
    logger.info("Bắt đầu xử lý dữ liệu sau khi Sensor xác nhận tín hiệu hợp lệ!")
    logger.info("Processing date partition: %s", logical_date)
    logger.info("Dữ liệu đã được nạp vào buffer thành công.")


default_args: Dict[str, Any] = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="02_sensors_demo",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Kích hoạt thủ công hoặc qua external trigger
    catchup=False,
    max_active_runs=1,
    tags=["module_02", "sensors", "event_driven"],
    doc_md=DAG_DOC_MD,
) as dag:

    # 1. PythonSensor với mode='reschedule' giúp tiết kiệm tuyệt đối tài nguyên Worker Slot
    wait_for_api_ready = PythonSensor(
        task_id="wait_for_api_ready",
        python_callable=check_external_system_ready,
        poke_interval=15,  # Thử lại sau mỗi 15 giây
        timeout=300,  # Hết thời gian chờ sau 5 phút
        mode="reschedule",  # GIẢI PHÓNG WORKER SLOT trong thời gian chờ
        exponential_backoff=True,  # Giãn thời gian poke theo cấp số nhân nếu thất bại
        soft_fail=False,  # Nếu timeout thì báo lỗi FAILED nghiêm trọng
        doc_md="Chờ phản hồi API bên ngoài theo cơ chế reschedule để không chiếm dụng worker slot.",
    )

    # 2. Xử lý sau khi sensor hoàn tất điều kiện
    process_data = PythonOperator(
        task_id="process_data_after_sensor",
        python_callable=process_landed_data,
        provide_context=True,
        doc_md="Xử lý luồng dữ liệu chính sau khi Sensor hoàn thành kiểm tra.",
    )

    # Thiết lập chuỗi thứ tự
    wait_for_api_ready >> process_data
