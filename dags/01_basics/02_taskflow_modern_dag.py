"""
==============================================================================
MODULE 01: TASKFLOW API (PHONG CÁCH AIRFLOW HIỆN ĐẠI 2.x+)
==============================================================================
Mục tiêu bài học:
1. Sử dụng Decorators `@dag` và `@task` thay thế class-based Operators truyền thống.
2. Tự động truyền dữ liệu giữa các task (XCom ngầm) theo chuẩn Pythonic.
3. Áp dụng Type Hinting (typing) giúp pipeline an toàn và tự tài liệu hóa.
4. Quản lý metadata, doc_md và xử lý ngoại lệ trong từng task function.
==============================================================================
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow.decorators import dag, task

logger = logging.getLogger("airflow.task")

default_args: Dict[str, Any] = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=20),
}

PIPELINE_DOC_MD = """
### ⚡ DAG 02: TaskFlow Modern Pipeline (Airflow 2.x+)
Minh họa phong cách viết DAG hiện đại bằng `@dag` và `@task` Decorators:
1. **Extract**: Thu thập dữ liệu người dùng dưới dạng Dictionary có kiểu dữ liệu chặt chẽ.
2. **Transform**: Phân loại cấp bậc (Tier: VIP/STANDARD) và chuẩn hóa cờ trạng thái.
3. **Load**: Mô phỏng nạp dữ liệu vào kho dữ liệu đích (Data Warehouse).
"""


@dag(
    dag_id="02_taskflow_modern_dag",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["module_01", "taskflow_api", "modern_etl"],
    doc_md=PIPELINE_DOC_MD,
)
def taskflow_pipeline() -> None:
    """DAG mẫu định nghĩa bằng TaskFlow API.

    Dữ liệu được pass tự nhiên qua tham số hàm (tự động tạo XComs dưới nền).
    """

    @task(task_id="extract_user_record")
    def extract_user_data() -> Dict[str, Any]:
        """Trích xuất dữ liệu người dùng mẫu (mô phỏng nguồn API / Database).

        Returns:
            Dict[str, Any]: Dữ liệu người dùng trích xuất.
        """
        logger.info("Đang bắt đầu trích xuất thông tin người dùng...")
        user_info: Dict[str, Any] = {
            "user_id": 101,
            "username": "minh_doan",
            "score": 88,
            "registered_at": "2024-05-15",
            "department": "Data Engineering",
        }
        logger.info("Trích xuất thành công: user_id=%s, score=%s", user_info["user_id"], user_info["score"])
        return user_info

    @task(task_id="transform_user_profile")
    def transform_user_data(user_info: Dict[str, Any]) -> Dict[str, Any]:
        """Xử lý, phân hạng và làm giàu dữ liệu người dùng.

        Args:
            user_info (Dict[str, Any]): Dữ liệu thô từ bước extract.

        Returns:
            Dict[str, Any]: Dữ liệu đã chuẩn hóa và phân hạng.
        """
        logger.info("Bắt đầu xử lý dữ liệu cho user_id=%s", user_info.get("user_id"))
        score = user_info.get("score", 0)
        tier = "VIP" if score >= 80 else "STANDARD"

        enriched_profile = dict(user_info)
        enriched_profile["tier"] = tier
        enriched_profile["is_active"] = True
        enriched_profile["processed_at"] = datetime.utcnow().isoformat()

        logger.info("Xử lý hoàn tất. Phân hạng: %s", tier)
        return enriched_profile

    @task(task_id="load_to_data_warehouse")
    def load_user_data(final_data: Dict[str, Any]) -> None:
        """Nạp dữ liệu đã xử lý vào Data Warehouse / Analytics Storage.

        Args:
            final_data (Dict[str, Any]): Dữ liệu sạch sẵn sàng phân tích.
        """
        logger.info("Đang nạp hồ sơ người dùng vào Data Warehouse...")
        logger.info(
            "User ID: %s | User: %s | Tier: %s | Status: Active",
            final_data["user_id"],
            final_data["username"],
            final_data["tier"],
        )
        logger.info("Pipeline TaskFlow thực thi thành công mỹ mãn!")

    # Thiết lập luồng phụ thuộc dữ liệu tự động (TaskFlow Data Dependency)
    raw_record = extract_user_data()
    transformed_profile = transform_user_data(raw_record)
    load_user_data(transformed_profile)


# Khởi tạo thể hiện DAG
taskflow_pipeline()
