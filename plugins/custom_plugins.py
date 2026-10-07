"""
==============================================================================
MODULE PLUGINS: ENTERPRISE AIRFLOW PLUGIN EXTENSION
==============================================================================
Mục tiêu bài học:
1. Hiểu cơ chế Plugin Manager phát hiện và nạp tự động từ thư mục plugins/.
2. Xây dựng Custom Hook kế thừa BaseHook để quản lý thông tin kết nối và retry.
3. Xây dựng Custom Operator kế thừa BaseOperator với template_fields và UI colors.
4. Triển khai BaseOperatorLink tạo nút liên kết ngoài (External Link) trên Web UI.
5. Đăng ký Custom Macro và đóng gói toàn diện qua AirflowPlugin.
==============================================================================
"""

import logging
import time
from typing import Any, Dict, Optional, Sequence

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.models.taskinstance import TaskInstance
from airflow.plugins_manager import AirflowPlugin

logger = logging.getLogger("airflow.plugins.audit")


class ExternalDocumentationLink(BaseOperatorLink):
    """Tạo nút liên kết trực tiếp trên Airflow Web UI dẫn tới hệ thống Wiki / Monitoring."""

    name = "📖 Audit Docs"

    def get_link(self, operator: BaseOperator, *, ti_key: Any) -> str:
        """Trả về URL tương ứng với task instance đang được chọn."""
        return f"https://github.com/doanquangminh14/Airflow?task={operator.task_id}"


class CustomAuditHook(BaseHook):
    """Hook tùy chỉnh quản lý kết nối và ghi log kiểm toán (Audit Trail) cho Data Platform."""

    conn_name_attr = "custom_conn_id"
    default_conn_name = "audit_service_default"
    conn_type = "audit_service"
    hook_name = "Custom Audit Service Hook"

    def __init__(self, custom_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.custom_conn_id = custom_conn_id

    def get_conn(self) -> Dict[str, Any]:
        """Lấy thông tin xác thực an toàn từ Airflow Connections Backend."""
        try:
            conn = self.get_connection(self.custom_conn_id)
            return {
                "host": conn.host or "audit-service.internal",
                "port": conn.port or 8443,
                "login": conn.login or "audit_client",
                "status": "CONNECTED",
            }
        except Exception as exc:
            logger.info("Chưa định nghĩa connection '%s', sử dụng cấu hình mặc định: %s", self.custom_conn_id, exc)
            return {
                "host": "audit-mock.local",
                "port": 8443,
                "login": "guest",
                "status": "MOCK_MODE",
            }

    def emit_audit_event(self, event_name: str, payload: Dict[str, Any]) -> bool:
        """Mô phỏng gửi sự kiện kiểm toán tới Audit Log Server hoặc Kafka topic."""
        conn_info = self.get_conn()
        logger.info(
            "[AUDIT EMITTER] Server: %s:%s | Event: %s | Payload: %s",
            conn_info["host"],
            conn_info["port"],
            event_name,
            payload,
        )
        return True


class CustomGreetingOperator(BaseOperator):
    """Operator tùy chỉnh gửi lời chào, ghi nhận audit trail và tính toán metrics."""

    template_fields: Sequence[str] = ("recipient_name", "audit_note")
    ui_color: str = "#ede7f6"
    ui_fgcolor: str = "#311b92"
    operator_extra_links = (ExternalDocumentationLink(),)

    def __init__(
        self,
        recipient_name: str,
        message: str = "Chào mừng bạn đến với Airflow",
        audit_note: str = "Normal operation",
        custom_conn_id: str = "audit_service_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.recipient_name = recipient_name
        self.message = message
        self.audit_note = audit_note
        self.custom_conn_id = custom_conn_id

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Thực thi logic chào hỏi và phát sự kiện kiểm toán qua Hook."""
        start_ts = time.time()
        greeting = f"[{self.task_id}] {self.message}, {self.recipient_name}!"
        logger.info(greeting)

        # Sử dụng CustomAuditHook để lưu log kiểm toán
        hook = CustomAuditHook(custom_conn_id=self.custom_conn_id)
        hook.emit_audit_event(
            event_name="GREETING_EXECUTED",
            payload={
                "task_id": self.task_id,
                "recipient": self.recipient_name,
                "audit_note": self.audit_note,
                "duration_ms": round((time.time() - start_ts) * 1000, 2),
            },
        )

        return {
            "greeting": greeting,
            "word_count": len(greeting.split()),
            "status": "SUCCESS",
        }


def format_filesize_bytes(size_in_bytes: int) -> str:
    """Custom Jinja Macro chuyển đổi số byte sang dung lượng người dùng dễ đọc (KB/MB/GB)."""
    if size_in_bytes < 1024:
        return f"{size_in_bytes} B"
    elif size_in_bytes < 1024 * 1024:
        return f"{size_in_bytes / 1024:.2f} KB"
    elif size_in_bytes < 1024 * 1024 * 1024:
        return f"{size_in_bytes / (1024 * 1024):.2f} MB"
    return f"{size_in_bytes / (1024 * 1024 * 1024):.2f} GB"


class CustomEnterprisePlugin(AirflowPlugin):
    """Đóng gói toàn diện Plugin để Airflow Plugin Manager tự động nhận diện."""

    name = "custom_enterprise_plugin"
    operators = [CustomGreetingOperator]
    hooks = [CustomAuditHook]
    macros = [format_filesize_bytes]
    operator_extra_links = [ExternalDocumentationLink()]
