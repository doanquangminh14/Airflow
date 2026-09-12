"""
==============================================================================
MODULE 03: AIRFLOW VARIABLES & CONNECTIONS
==============================================================================
Mục tiêu bài học:
1. Cách sử dụng `Variable.get()` an toàn (tránh gọi ở top-level code).
2. Lấy thông tin kết nối từ `BaseHook.get_connection()`.
3. Best practice: Tránh làm nghẽn DB kết nối của Scheduler bằng cách chỉ gọi trong execute/task callable.
"""

from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

def access_configuration_and_secrets(**kwargs):
    """
    Truy cập Variables và Connections bên trong task runtime.
    LƯU Ý: Không bao giờ gọi Variable.get() ở ngoài hàm (top-level code) vì sẽ làm
    Scheduler query database liên tục mỗi giây!
    """
    # 1. Đọc Variable với giá trị fallback mặc định
    app_env = Variable.get("APP_ENVIRONMENT", default_var="development")
    api_rate_limit = Variable.get("API_RATE_LIMIT", default_var=100, deserialize_json=False)

    print(f"[CONFIGURATION] Môi trường: {app_env} | Rate limit: {api_rate_limit}")

    # 2. Truy cập Connection một cách an toàn
    try:
        conn = BaseHook.get_connection("postgres_default")
        print(f"[CONNECTION] Host: {conn.host}, Port: {conn.port}, Schema: {conn.schema}")
    except Exception as e:
        print(f"[CONNECTION NOTE] Chưa cấu hình postgres_default trong UI, đang dùng cấu hình test. Chi tiết: {e}")

with DAG(
    dag_id="02_variables_connections",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["module_03", "variables", "connections", "security"],
) as dag:

    read_config_task = PythonOperator(
        task_id="read_configs_and_secrets",
        python_callable=access_configuration_and_secrets,
    )
