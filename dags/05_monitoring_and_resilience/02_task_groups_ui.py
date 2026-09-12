"""
==============================================================================
MODULE 05: TASK GROUPS - TỔ CHỨC DAG TRỰC QUAN TRÊN GIAO DIỆN
==============================================================================
Mục tiêu bài học:
1. Sử dụng `TaskGroup` để gom nhóm các task liên quan lại với nhau trên UI.
2. Tránh sử dụng `SubDAG` (SubDAG đã bị deprecated trong Airflow 2.x do gây deadlock).
3. Sử dụng decorator `@task_group` hoặc TaskGroup Context Manager.
"""

from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="02_task_groups_ui",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["module_05", "task_group", "ui_clean"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # 1. Nhóm tác vụ Trích xuất (Extract Group)
    with TaskGroup("extract_sources_group", tooltip="Trích xuất dữ liệu từ các nguồn khác nhau") as extract_group:
        extract_db = BashOperator(task_id="extract_from_db", bash_command="echo 'Extracting DB...'")
        extract_api = BashOperator(task_id="extract_from_api", bash_command="echo 'Extracting API...'")
        extract_files = BashOperator(task_id="extract_from_s3", bash_command="echo 'Extracting S3...'")

    # 2. Nhóm tác vụ Xử lý & Làm sạch (Transform Group)
    with TaskGroup("transform_data_group", tooltip="Làm sạch và tổng hợp dữ liệu") as transform_group:
        clean_nulls = BashOperator(task_id="clean_null_values", bash_command="echo 'Cleaning nulls...'")
        deduplicate = BashOperator(task_id="remove_duplicates", bash_command="echo 'Removing duplicates...'")
        aggregate = BashOperator(task_id="aggregate_metrics", bash_command="echo 'Aggregating...'")
        
        [clean_nulls, deduplicate] >> aggregate

    # 3. Liên kết các nhóm với nhau
    start >> extract_group >> transform_group >> end
