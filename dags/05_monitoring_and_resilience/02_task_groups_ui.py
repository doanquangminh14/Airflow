"""
==============================================================================
MODULE 05: TASK GROUPS - TỔ CHỨC DAG TRỰC QUAN TRÊN GIAO DIỆN
==============================================================================
Mục tiêu bài học:
1. Sử dụng `TaskGroup` để gom nhóm các task liên quan lại với nhau trên Airflow Web UI.
2. Hiểu vì sao TaskGroup thay thế hoàn toàn `SubDAG` (SubDAG đã bị loại bỏ vì gây deadlock worker).
3. Thiết kế cấu trúc TaskGroup lồng nhau (Nested TaskGroups) cho pipeline phức tạp.
4. Tùy biến tooltip, prefix_group_id và điều phối quan hệ giữa các groups.
==============================================================================
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

logger = logging.getLogger("airflow.task")

DAG_DOC_MD = """
### 🗂️ DAG: Hierarchical Task Groups UI Pattern
Minh họa cách tổ chức giao diện DAG chuyên nghiệp với hàng chục tasks:
- **`extract_sources_group`**: Gom các tác vụ cào/trích xuất từ nhiều nguồn (DB, API, S3).
- **`transform_data_group`**: Nhóm xử lý dữ liệu cấp cao, bên trong chứa nhóm con (Nested Group):
  - **`clean_group`**: Xóa null, lọc trùng.
  - **`enrich_group`**: Làm giàu dữ liệu, tính toán chỉ số tổng hợp.
- **`load_dw_group`**: Tác vụ nạp vào Data Warehouse.
"""

default_args: Dict[str, Any] = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="02_task_groups_ui",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["module_05", "task_group", "ui_clean", "best_practices"],
    doc_md=DAG_DOC_MD,
) as dag:

    start_pipeline = EmptyOperator(
        task_id="start_pipeline",
        doc_md="Khởi động pipeline tổng thể.",
    )
    end_pipeline = EmptyOperator(
        task_id="end_pipeline",
        doc_md="Kết thúc toàn bộ pipeline.",
    )

    # 1. Nhóm tác vụ Trích xuất (Extract Group)
    with TaskGroup("extract_sources_group", tooltip="Trích xuất dữ liệu đa nguồn (DB, API, Cloud Storage)") as extract_group:
        extract_db = BashOperator(
            task_id="extract_from_db",
            bash_command="echo 'Extracting records from PostgreSQL Warehouse...'",
        )
        extract_api = BashOperator(
            task_id="extract_from_api",
            bash_command="echo 'Fetching metrics from Third-party REST API...'",
        )
        extract_files = BashOperator(
            task_id="extract_from_s3",
            bash_command="echo 'Downloading parquet files from S3 Datalake...'",
        )

    # 2. Nhóm tác vụ Xử lý & Làm sạch lồng nhau (Nested Transform Group)
    with TaskGroup("transform_data_group", tooltip="Quy trình biến đổi, làm sạch và tổng hợp dữ liệu") as transform_group:

        # Nhóm con 1: Làm sạch
        with TaskGroup("cleaning_subgroup", tooltip="Lọc bỏ dữ liệu rác và trùng lặp") as clean_subgroup:
            clean_nulls = BashOperator(
                task_id="clean_null_values",
                bash_command="echo 'Imputing null values in key columns...'",
            )
            deduplicate = BashOperator(
                task_id="remove_duplicates",
                bash_command="echo 'Removing duplicated primary keys...'",
            )
            [clean_nulls, deduplicate]

        # Nhóm con 2: Làm giàu dữ liệu
        with TaskGroup("enrichment_subgroup", tooltip="Tính toán chỉ số và gắn nhãn phân loại") as enrich_subgroup:
            aggregate_metrics = BashOperator(
                task_id="aggregate_metrics",
                bash_command="echo 'Aggregating KPI and sales volume...'",
            )

        # Thứ tự trong nhóm Transform: Làm sạch xong mới tổng hợp
        clean_subgroup >> enrich_subgroup

    # 3. Nhóm nạp kho dữ liệu đích
    with TaskGroup("load_dw_group", tooltip="Nạp dữ liệu vào bảng Staging và Production") as load_group:
        load_staging = BashOperator(
            task_id="load_to_staging",
            bash_command="echo 'COPY INTO staging_orders FROM @s3_stage...'",
        )
        merge_production = BashOperator(
            task_id="merge_into_production",
            bash_command="echo 'MERGE INTO fact_orders USING staging_orders...'",
        )
        load_staging >> merge_production

    # 4. Liên kết tuần tự giữa các TaskGroups lớn
    start_pipeline >> extract_group >> transform_group >> load_group >> end_pipeline
