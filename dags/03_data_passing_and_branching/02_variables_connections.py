"""
==============================================================================
MODULE 03: AIRFLOW VARIABLES & CONNECTIONS
==============================================================================
Mục tiêu bài học:
1. Sử dụng `Variable.get()` an toàn và hiệu quả với JSON deserialization.
2. Khai thác thông tin kết nối an toàn từ `BaseHook.get_connection()`.
3. So sánh việc truy xuất qua Jinja template `{{ var.value.my_var }}` vs Python runtime.
4. Triển khai Secret Masking bảo vệ thông tin nhạy cảm khỏi log hiển thị.
==============================================================================
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

DAG_DOC_MD = """
### 🔐 DAG: Variables & Connections Best Practices
Hướng dẫn quản trị cấu hình và thông tin mật (Secrets) trong Airflow:
- **Variable.get() an toàn**: Luôn đặt giá trị mặc định (`default_var`) và chỉ gọi trong runtime callable.
- **BaseHook.get_connection()**: Đọc cấu hình database host, schema, port mà không hardcode trong mã nguồn.
- **Performance Alert**: Tránh tuyệt đối việc gọi `Variable.get()` ở phạm vi Top-level của file script.
"""


def access_configuration_and_secrets(**context: Any) -> None:
    """Truy cập Variables và Connections bên trong task runtime.

    LƯU Ý QUAN TRỌNG:
    Không bao giờ gọi Variable.get() ở ngoài hàm (top-level code) vì Scheduler
    quét file định kỳ sẽ thực hiện hàng trăm câu query làm sập Metadata DB!
    """
    logger.info("=" * 60)
    logger.info("Đang truy xuất cấu hình toàn cục từ Airflow Variables...")

    # 1. Đọc String Variable với giá trị fallback mặc định
    app_env = Variable.get("APP_ENVIRONMENT", default_var="production")

    # 2. Đọc JSON Variable (cấu hình phức tạp)
    default_config = {"batch_size": 1000, "rate_limit_per_sec": 50, "enable_retries": True}
    pipeline_config = Variable.get("PIPELINE_TUNING_CONFIG", default_var=default_config, deserialize_json=True)

    logger.info("Môi trường thực thi: %s", app_env)
    logger.info("Cấu hình Tuning: batch_size=%s, rate_limit=%s", pipeline_config.get("batch_size"), pipeline_config.get("rate_limit_per_sec"))

    # 3. Truy cập Connection một cách an toàn qua BaseHook
    try:
        conn = BaseHook.get_connection("postgres_default")
        logger.info(
            "Kết nối Connection thành công: Conn ID=%s, Host=%s, Port=%s, Schema=%s",
            conn.conn_id,
            conn.host or "localhost",
            conn.port or 5432,
            conn.schema or "analytics",
        )
    except Exception as exc:
        logger.warning(
            "Chưa cấu hình postgres_default trong Airflow UI/Secrets Backend. "
            "Sử dụng fallback demo thành công. Chi tiết lỗi: %s",
            exc,
        )
    logger.info("=" * 60)


default_args: Dict[str, Any] = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="02_variables_connections",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["module_03", "variables", "connections", "security"],
    doc_md=DAG_DOC_MD,
) as dag:

    read_config_task = PythonOperator(
        task_id="read_configs_and_secrets",
        python_callable=access_configuration_and_secrets,
        provide_context=True,
        doc_md="Truy xuất và kiểm tra Airflow Variables & Connection Credentials an toàn.",
    )
