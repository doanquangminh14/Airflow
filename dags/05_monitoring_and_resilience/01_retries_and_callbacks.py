"""
==============================================================================
MODULE 05: RETRIES, CALLBACKS & CẢNH BÁO LỖI (ALERTING)
==============================================================================
Mục tiêu bài học:
1. Cấu hình cơ chế tự động thử lại chuẩn Production (retries, retry_exponential_backoff, max_retry_delay).
2. Xây dựng Callback Functions: on_failure_callback, on_success_callback, on_retry_callback, sla_miss_callback.
3. Trích xuất metadata sự cố (task_id, try_number, log_url, exception) gửi về Webhook Slack/Discord/Teams.
4. Đảm bảo callback không làm sập task khi hệ thống cảnh báo gặp sự cố mạng tạm thời.
==============================================================================
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

DAG_DOC_MD = """
### 🚨 DAG: Resilience, Exponential Backoff & Callbacks
Minh họa thiết kế pipeline chịu lỗi cao trong môi trường Production:
- **Exponential Backoff**: Tự động tăng thời gian giãn cách giữa các lần thử lại ($10s \to 20s \to 40s$) giúp server đích hồi phục.
- **Callback Handlers**: Tự động kích hoạt khi task đổi trạng thái sang SUCCESS, UP_FOR_RETRY hoặc FAILED.
- **Webhook Notifier Payload**: Tạo sẵn cấu trúc JSON tích hợp gửi thông báo Slack / Teams / Discord.
"""


def build_alert_payload(context: Dict[str, Any], event_type: str) -> Dict[str, Any]:
    """Tạo payload chuẩn hóa để gửi sang hệ thống giám sát / ChatOps Webhook."""
    ti = context.get("task_instance")
    dag = context.get("dag")
    logical_date = context.get("logical_date")
    exception = context.get("exception")

    task_id = ti.task_id if ti else "unknown_task"
    dag_id = dag.dag_id if dag else "unknown_dag"
    try_num = ti.try_number if ti else 1
    max_tries = (ti.max_tries + 1) if ti else 1
    log_url = ti.log_url if ti else ""

    payload = {
        "event": event_type,
        "dag_id": dag_id,
        "task_id": task_id,
        "try_number": f"{try_num}/{max_tries}",
        "logical_date": str(logical_date),
        "log_url": log_url,
        "error_message": str(exception) if exception else None,
        "timestamp": datetime.utcnow().isoformat(),
    }
    return payload


def on_failure_handler(context: Dict[str, Any]) -> None:
    """Callback được gọi khi task hoặc DAG hoàn toàn thất bại (hết số lần retry)."""
    payload = build_alert_payload(context, event_type="FAILED ❌")
    logger.critical("=" * 60)
    logger.critical("[ALERT BOT] PHÁT HIỆN SỰ CỐ NGHIÊM TRỌNG TRONG PIPELINE:")
    logger.critical("DAG: %s | Task: %s | Lần thử: %s", payload["dag_id"], payload["task_id"], payload["try_number"])
    logger.critical("Log URL để điều tra: %s", payload["log_url"])
    logger.critical("Chi tiết lỗi: %s", payload["error_message"])
    logger.critical("Đã gửi thông báo khẩn cấp tới On-Call Data Engineer qua Slack!")
    logger.critical("=" * 60)


def on_success_handler(context: Dict[str, Any]) -> None:
    """Callback được gọi khi task hoàn thành thành công."""
    payload = build_alert_payload(context, event_type="SUCCESS ✅")
    logger.info("[ALERT BOT] Task %s trong DAG %s đã thực thi THÀNH CÔNG!", payload["task_id"], payload["dag_id"])


def on_retry_handler(context: Dict[str, Any]) -> None:
    """Callback được gọi mỗi khi task gặp sự cố và chuẩn bị thử lại (UP_FOR_RETRY)."""
    payload = build_alert_payload(context, event_type="RETRYING 🔄")
    logger.warning(
        "[ALERT BOT] Task %s gặp lỗi tạm thời (Thử lần %s). Đang kích hoạt Exponential Backoff để thử lại...",
        payload["task_id"],
        payload["try_number"],
    )


def simulate_network_api_call(**context: Any) -> str:
    """Giả lập task kết nối API bên thứ ba có độ trễ hoặc lỗi mạng."""
    logger.info("Đang kết nối tới External Payment Gateway API...")
    # Giả lập hoàn tất thành công
    return "API Request Succeeded with HTTP 200 OK"


default_args: Dict[str, Any] = {
    "owner": "data_engineer",
    "retries": 3,  # Cho phép thử lại 3 lần
    "retry_delay": timedelta(seconds=10),  # Thời gian chờ ban đầu 10 giây
    "retry_exponential_backoff": True,  # Giãn thời gian theo cấp số nhân
    "max_retry_delay": timedelta(minutes=5),  # Giới hạn trần không vượt quá 5 phút
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="01_retries_and_callbacks",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    # Gắn handler cấp độ DAG (khi bất kỳ task nào trong DAG fail mà không xử lý nội bộ)
    on_failure_callback=on_failure_handler,
    tags=["module_05", "monitoring", "callbacks", "retries"],
    doc_md=DAG_DOC_MD,
) as dag:

    resilient_task = PythonOperator(
        task_id="resilient_api_call",
        python_callable=simulate_network_api_call,
        on_failure_callback=on_failure_handler,
        on_success_callback=on_success_handler,
        on_retry_callback=on_retry_handler,
        doc_md="Thực thi gọi API an toàn với bộ 3 Callbacks: Success, Failure, Retry.",
    )
