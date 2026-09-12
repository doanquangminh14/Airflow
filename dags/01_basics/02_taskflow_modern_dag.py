"""
==============================================================================
MODULE 01: TASKFLOW API (PHONG CÁCH AIRFLOW HIỆN ĐẠI 2.x+)
==============================================================================
Mục tiêu bài học:
1. Sử dụng Decorators `@dag` và `@task` thay cho class-based Operators.
2. Tự động truyền dữ liệu giữa các task mà không cần cú pháp XCom rườm rà.
3. Code sạch sẽ, dễ đọc như Python thông thường.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task

# Cấu hình mặc định cho DAG
default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="02_taskflow_modern_dag",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["module_01", "taskflow_api", "modern"],
)
def taskflow_pipeline():
    """
    DAG mẫu định nghĩa bằng TaskFlow API.
    Dữ liệu được pass tự nhiên qua tham số hàm (tự động tạo XComs dưới nền).
    """

    @task()
    def extract_user_data() -> dict:
        """Trích xuất dữ liệu người dùng (giả lập)."""
        user_info = {
            "user_id": 101,
            "username": "minh_doan",
            "score": 88,
            "registered_at": "2024-05-15",
        }
        print(f"Extracted user data: {user_info}")
        return user_info

    @task()
    def transform_user_data(user_info: dict) -> dict:
        """Xử lý và tính toán thêm thuộc tính cho người dùng."""
        score = user_info["score"]
        tier = "VIP" if score >= 80 else "STANDARD"
        user_info["tier"] = tier
        user_info["is_active"] = True
        print(f"Transformed user data: {user_info}")
        return user_info

    @task()
    def load_user_data(final_data: dict) -> None:
        """Lưu trữ dữ liệu vào cơ sở dữ liệu hoặc hệ thống đích."""
        print(f"Nạp dữ liệu người dùng vào Database: ID={final_data['user_id']}, Tier={final_data['tier']}")
        print("Pipeline TaskFlow hoàn thành xuất sắc!")

    # Gọi các hàm để tạo luồng phụ thuộc dữ liệu tự động
    raw_data = extract_user_data()
    processed_data = transform_user_data(raw_data)
    load_user_data(processed_data)

# Khởi tạo thể hiện của DAG
taskflow_pipeline()
