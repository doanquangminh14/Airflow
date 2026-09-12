"""
Custom Plugins, Operators and Hooks for Apache Airflow.

Airflow phát hiện các plugin nằm trong thư mục plugins/ để mở rộng tính năng,
kết nối với hệ thống nội bộ hoặc tạo các custom operator dùng chung.
"""

from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import logging

logger = logging.getLogger(__name__)


class CustomAuditHook(BaseHook):
    """
    Hook tùy chỉnh để ghi log kiểm toán (Audit Trail) cho các tác vụ Airflow.
    """
    conn_name_attr = "custom_conn_id"
    default_conn_name = "custom_default"
    conn_type = "custom_service"
    hook_name = "Custom Audit Hook"

    def __init__(self, custom_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.custom_conn_id = custom_conn_id

    def log_event(self, event_name: str, payload: dict) -> bool:
        """Mô phỏng gửi log audit đến hệ thống giám sát hoặc log server."""
        logger.info(f"[AUDIT HOOK] Event: {event_name} | Payload: {payload}")
        return True


class CustomGreetingOperator(BaseOperator):
    """
    Operator tùy chỉnh in lời chào và thống kê số từ đã xử lý.
    """
    template_fields = ("recipient_name",)

    def __init__(self, recipient_name: str, message: str = "Chào mừng bạn đến với Airflow", **kwargs):
        super().__init__(**kwargs)
        self.recipient_name = recipient_name
        self.message = message

    def execute(self, context):
        greeting = f"[{self.task_id}] {self.message}, {self.recipient_name}!"
        logger.info(greeting)
        word_count = len(greeting.split())
        # Trả về kết quả (tự động đưa vào XCom)
        return {"greeting": greeting, "word_count": word_count}


class CustomLearningPlugin(AirflowPlugin):
    name = "custom_learning_plugin"
    operators = [CustomGreetingOperator]
    hooks = [CustomAuditHook]
