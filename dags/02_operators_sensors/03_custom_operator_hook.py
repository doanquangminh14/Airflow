"""
==============================================================================
MODULE 02: TẠO VÀ SỬ DỤNG CUSTOM OPERATOR & HOOK
==============================================================================
Mục tiêu bài học:
1. Cách tự định nghĩa một Custom Operator kế thừa BaseOperator.
2. Template fields (hỗ trợ Jinja expression).
3. Sử dụng Plugin / Custom Operator bên trong DAG.
"""

from datetime import datetime
from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import PythonOperator

class CleanAndValidateCsvOperator(BaseOperator):
    """
    Custom Operator kiểm tra định dạng dữ liệu mẫu.
    """
    template_fields = ("source_name",)

    def __init__(self, source_name: str, min_rows: int = 1, **kwargs):
        super().__init__(**kwargs)
        self.source_name = source_name
        self.min_rows = min_rows

    def execute(self, context):
        self.log.info(f"Đang làm sạch và kiểm tra dữ liệu từ nguồn: {self.source_name}")
        simulated_row_count = 150
        if simulated_row_count < self.min_rows:
            raise ValueError(f"Số lượng dòng {simulated_row_count} nhỏ hơn ngưỡng tối thiểu {self.min_rows}")
        
        self.log.info(f"Kiểm tra thành công! Tổng số dòng hợp lệ: {simulated_row_count}")
        return {"source": self.source_name, "valid_rows": simulated_row_count}

with DAG(
    dag_id="03_custom_operator_hook",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["module_02", "custom_operator", "plugins"],
) as dag:

    # Sử dụng Custom Operator với Jinja template
    validate_sales_data = CleanAndValidateCsvOperator(
        task_id="validate_sales_data",
        source_name="sales_report_{{ ds }}.csv",
        min_rows=10,
    )

    def summarize_results(**context):
        ti = context["ti"]
        result = ti.xcom_pull(task_ids="validate_sales_data")
        print(f"Tổng kết từ Custom Operator: {result}")

    report_summary = PythonOperator(
        task_id="report_summary",
        python_callable=summarize_results,
    )

    validate_sales_data >> report_summary
