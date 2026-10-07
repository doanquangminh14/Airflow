"""
==============================================================================
MODULE 02: CÁC OPERATORS PHỔ BIẾN TRONG AIRFLOW
==============================================================================
Mục tiêu bài học:
1. Nắm vững BashOperator, PythonOperator và EmptyOperator.
2. Ứng dụng Jinja Templating và Airflow Built-in Macros (ds, ds_nodash, dag_id).
3. Đăng ký và sử dụng User-Defined Macros & Filters trong DAG definition.
4. Truyền tham số an toàn qua op_kwargs và biến môi trường (env).
==============================================================================
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

DAG_DOC_MD = """
### ⚙️ DAG: Standard Operators & Jinja Templating
Minh họa cách sử dụng các toán tử nền tảng trong Airflow:
- **EmptyOperator**: Đóng vai trò là Start / End anchor nodes giúp đồ thị DAG rõ ràng.
- **BashOperator**: Thực thi shell command với các biến Jinja macro (`{{ ds }}`, `{{ ds_nodash }}`).
- **PythonOperator**: Nhận tham số qua `op_kwargs` và định dạng số tiền qua user-defined macro.
"""


def format_vnd(amount: int) -> str:
    """User-defined macro để định dạng tiền tệ Việt Nam Đồng."""
    return f"{amount:,.0f} VND"


def compute_metrics(category: str, multiplier: int, **kwargs: Any) -> Dict[str, Any]:
    """Xử lý tính toán nhận tham số từ op_kwargs và đọc Jinja context."""
    logical_date_str = kwargs.get("ds", "N/A")
    total_val = 100_000 * multiplier

    logger.info("=" * 50)
    logger.info("Executing compute_metrics task")
    logger.info("Logical Date: %s", logical_date_str)
    logger.info("Category: %s | Multiplier: %s | Total: %s", category, multiplier, total_val)
    logger.info("=" * 50)

    return {
        "category": category,
        "raw_total": total_val,
        "formatted_total": format_vnd(total_val),
        "execution_date": logical_date_str,
    }


default_args: Dict[str, Any] = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="01_standard_operators",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["module_02", "operators", "templating"],
    doc_md=DAG_DOC_MD,
    user_defined_macros={"format_currency": format_vnd},
) as dag:

    # 1. Điểm bắt đầu luồng dữ liệu (Anchor Point)
    start_node = EmptyOperator(
        task_id="start_pipeline",
        doc_md="Điểm neo đồng bộ hóa bắt đầu toàn bộ pipeline.",
    )

    # 2. BashOperator sử dụng Jinja templating & biến môi trường
    bash_task = BashOperator(
        task_id="run_bash_script",
        bash_command=(
            'echo "Date: {{ ds }} | No-Dash: {{ ds_nodash }} | '
            'DAG: {{ dag.dag_id }} | Env: $APP_ENV | Macro test: {{ format_currency(5000000) }}"'
        ),
        env={"APP_ENV": "Production_Cluster_US_East"},
        doc_md="Chạy shell script kiểm tra các macro Jinja và biến môi trường.",
    )

    # 3. PythonOperator truyền tham số qua op_kwargs
    calc_task = PythonOperator(
        task_id="compute_financial_metrics",
        python_callable=compute_metrics,
        op_kwargs={"category": "E-Commerce", "multiplier": 5},
        provide_context=True,
        doc_md="Xử lý tính toán chỉ số tài chính và ghi nhận kết quả vào metadata.",
    )

    # 4. Điểm kết thúc luồng dữ liệu
    end_node = EmptyOperator(
        task_id="end_pipeline",
        doc_md="Điểm neo đánh dấu toàn bộ các nhánh song song đã hoàn tất.",
    )

    # Thiết lập chuỗi phụ thuộc phân nhánh song song và hợp nhất
    start_node >> [bash_task, calc_task] >> end_node
