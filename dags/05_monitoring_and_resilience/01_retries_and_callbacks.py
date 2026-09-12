"""
==============================================================================
MODULE 05: RETRIES, CALLBACKS & CẢNH BÁO LỖI (ALERTING)
==============================================================================
Mục tiêu bài học:
1. Cấu hình cơ chế tự động thử lại (retries, retry_delay, retry_exponential_backoff).
2. Xây dựng Callback Functions: `on_failure_callback`, `on_success_callback`, `on_retry_callback`.
3. Tích hợp cảnh báo Telegram/Slack/Email khi pipeline gặp sự cố.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def send_alert_notification(context, status: str):
    """
    Hàm xử lý callback gửi thông báo khi task thành công hoặc thất bại.
    """
    task_id = context.get("task_instance").task_id
    dag_id = context.get("dag").dag_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    
    print(f"================ [NOTIFICATION SYSTEM: {status}] ================")
    print(f"DAG        : {dag_id}")
    print(f"Task       : {task_id}")
    print(f"Logical Date: {execution_date}")
    if exception:
        print(f"Lỗi gặp phải: {exception}")
    print("==================================================================")

def on_failure_handler(context):
    send_alert_notification(context, status="FAILED ❌")

def on_success_handler(context):
    send_alert_notification(context, status="SUCCESS ✅")

def on_retry_handler(context):
    send_alert_notification(context, status="RETRYING 🔄")

def risky_network_task():
    """Giả lập task kết nối API có thể chập chờn."""
    print("Đang thực hiện gọi API dịch vụ bên ngoài...")
    # Thao tác bình thường
    return "API Request Succeeded"

with DAG(
    dag_id="01_retries_and_callbacks",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    on_failure_callback=on_failure_handler,  # Áp dụng cấp độ toàn DAG
    tags=["module_05", "monitoring", "callbacks", "retries"],
) as dag:

    resilient_task = PythonOperator(
        task_id="resilient_api_call",
        python_callable=risky_network_task,
        retries=3,                              # Thử lại 3 lần
        retry_delay=timedelta(seconds=10),      # Chờ 10s trước khi thử lại
        retry_exponential_backoff=True,         # Tăng thời gian chờ theo hàm mũ
        max_retry_delay=timedelta(minutes=5),   # Giới hạn trần thời gian chờ
        on_failure_callback=on_failure_handler,
        on_success_callback=on_success_handler,
        on_retry_callback=on_retry_handler,
    )
