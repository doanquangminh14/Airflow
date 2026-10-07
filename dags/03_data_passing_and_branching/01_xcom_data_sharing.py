"""
==============================================================================
MODULE 03: XCOMS TRONG AIRFLOW (CROSS-COMMUNICATION)
==============================================================================
Mục tiêu bài học:
1. Hiểu cơ chế XCom: cách trao đổi metadata/small payloads giữa các task qua Metadata Database.
2. Sử dụng `ti.xcom_push` và `ti.xcom_pull` với key tùy chỉnh và nhiều task producer.
3. Kiến trúc Custom XCom Backend (S3/GCS) khi payload vượt quá giới hạn database.
4. Cảnh báo quan trọng: Tuyệt đối KHÔNG dùng XCom để truyền DataFrames lớn (> 48KB/SQLite hay 10MB/Postgres).
==============================================================================
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

DAG_DOC_MD = """
### 🔄 DAG: XCom Data Sharing Architecture
Minh họa cơ chế trao đổi siêu dữ liệu giữa các task:
- **Producer Task**: Đẩy return_value ngầm định và đẩy custom keys (`model_accuracy`, `processed_record_count`).
- **Consumer Task**: Kéo dữ liệu từ producer bằng `xcom_pull(task_ids=..., key=...)` và kiểm tra logic nghiệp vụ.
"""


def push_metrics_data(**context: Any) -> Dict[str, Any]:
    """Đẩy siêu dữ liệu và chỉ số chất lượng vào XCom qua TaskInstance."""
    ti = context["ti"]
    logical_date = context.get("ds", "N/A")

    logger.info("=" * 50)
    logger.info("Producer Task đang tính toán và đẩy metadata...")

    # 1. Đẩy custom keys phục vụ báo cáo / giám sát
    accuracy_score = 0.9542
    record_count = 15_420
    ti.xcom_push(key="model_accuracy", value=accuracy_score)
    ti.xcom_push(key="processed_record_count", value=record_count)
    ti.xcom_push(key="execution_partition", value=logical_date)

    logger.info("Đã lưu vào XCom: accuracy=%.4f, count=%s", accuracy_score, record_count)
    logger.info("=" * 50)

    # 2. Giá trị return_value tự động lưu vào XCom với key="return_value"
    return {
        "status": "SUCCESS",
        "pipeline_version": "v2.1.0",
        "storage_uri": f"s3://my-datalake-bucket/curated/{logical_date}/",
    }


def pull_metrics_data(**context: Any) -> None:
    """Kéo dữ liệu từ producer task thông qua TaskInstance."""
    ti = context["ti"]

    # Lấy return_value mặc định
    main_summary = ti.xcom_pull(task_ids="producer_task", key="return_value")

    # Lấy các giá trị theo custom key đã đăng ký
    accuracy = ti.xcom_pull(task_ids="producer_task", key="model_accuracy")
    records = ti.xcom_pull(task_ids="producer_task", key="processed_record_count")
    partition = ti.xcom_pull(task_ids="producer_task", key="execution_partition")

    logger.info("=" * 50)
    logger.info("Consumer Task đã nhận dữ liệu từ XCom thành công:")
    logger.info("Trạng thái Pipeline: %s", main_summary.get("status") if main_summary else "N/A")
    logger.info("Datalake Path: %s", main_summary.get("storage_uri") if main_summary else "N/A")
    logger.info("Model Accuracy: %.2f%%", (accuracy or 0) * 100)
    logger.info("Tổng số bản ghi: %s | Phân vùng: %s", records, partition)
    logger.info("=" * 50)


default_args: Dict[str, Any] = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="01_xcom_data_sharing",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["module_03", "xcom", "data_sharing"],
    doc_md=DAG_DOC_MD,
) as dag:

    producer = PythonOperator(
        task_id="producer_task",
        python_callable=push_metrics_data,
        provide_context=True,
        doc_md="Thực hiện tính toán và lưu trữ các metric metadata vào XCom database.",
    )

    consumer = PythonOperator(
        task_id="consumer_task",
        python_callable=pull_metrics_data,
        provide_context=True,
        doc_md="Truy xuất metadata từ producer task và tiến hành xác thực.",
    )

    producer >> consumer
