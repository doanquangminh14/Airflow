"""
==============================================================================
MODULE 02: CÁC OPERATORS PHỔ BIẾN TRONG AIRFLOW
==============================================================================
Mục tiêu bài học:
1. Nắm vững BashOperator và truyền tham số môi trường (env).
2. Sử dụng PythonOperator với op_args và op_kwargs.
3. Sử dụng EmptyOperator / DummyOperator để làm điểm neo (anchor points).
4. Jinja templating trong các thuộc tính operator.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

def compute_metrics(category: str, multiplier: int, **kwargs):
    """Xử lý tính toán nhận tham số từ op_kwargs."""
    execution_date_str = kwargs.get("ds")  # Jinja macro YYYY-MM-DD
    total = 100 * multiplier
    print(f"[{execution_date_str}] Danh mục: {category} | Tổng tính toán: {total}")
    return {"category": category, "total": total}

with DAG(
    dag_id="01_standard_operators",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["module_02", "operators"],
) as dag:

    # 1. Điểm bắt đầu luồng dữ liệu (Anchor Point)
    start_node = EmptyOperator(task_id="start_pipeline")

    # 2. BashOperator sử dụng Jinja templating & biến môi trường
    bash_task = BashOperator(
        task_id="run_bash_script",
        bash_command='echo "Ngày chạy: {{ ds }} | DAG ID: {{ dag.dag_id }} | Biến Env: $ENV_VAR"',
        env={"ENV_VAR": "Production_Environment_Ready"},
    )

    # 3. PythonOperator truyền tham số qua op_kwargs
    calc_task = PythonOperator(
        task_id="compute_financial_metrics",
        python_callable=compute_metrics,
        op_kwargs={"category": "E-Commerce", "multiplier": 5},
    )

    # 4. Điểm kết thúc luồng dữ liệu
    end_node = EmptyOperator(task_id="end_pipeline")

    # Thiết lập chuỗi phụ thuộc
    start_node >> [bash_task, calc_task] >> end_node
