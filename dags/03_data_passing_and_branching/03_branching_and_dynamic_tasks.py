"""
==============================================================================
MODULE 03: PHÂN NHÁNH (BRANCHING) & DYNAMIC TASK MAPPING
==============================================================================
Mục tiêu bài học:
1. Sử dụng BranchPythonOperator để điều hướng luồng dữ liệu theo điều kiện nghiệp vụ.
2. Thiết lập trigger_rule thích hợp (TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS) ở task hội tụ.
3. Sử dụng Dynamic Task Mapping (`@task.expand`) trong Airflow 2.3+ để song song hóa linh hoạt.
4. Tổng hợp dữ liệu từ dynamic tasks về downstream task an toàn.
==============================================================================
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger("airflow.task")

DAG_DOC_MD = """
### 🌿 DAG: Dynamic Mapping & Conditional Branching
Kết hợp 2 tính năng điều khiển luồng nâng cao trong Airflow:
1. **Dynamic Task Mapping (`.expand()`)**: Tự động sinh số lượng tasks con dựa vào danh sách trả về từ runtime.
2. **BranchPythonOperator**: Đưa ra quyết định rẽ nhánh dựa trên điều kiện thực tế (ví dụ: ngày chẵn/lẻ hoặc ngưỡng dữ liệu).
3. **Join Node với Trigger Rule**: Sử dụng `NONE_FAILED_MIN_ONE_SUCCESS` để không bị chuyển sang trạng thái SKIPPED do nhánh còn lại bị bỏ qua.
"""

default_args: Dict[str, Any] = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="03_branching_and_dynamic_tasks",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["module_03", "branching", "dynamic_mapping"],
    doc_md=DAG_DOC_MD,
)
def dynamic_and_branching_pipeline() -> None:

    # ------------------- PHẦN 1: DYNAMIC TASK MAPPING -------------------
    @task(task_id="get_source_regions")
    def get_source_regions() -> List[str]:
        """Tạo danh sách các khu vực thị trường cần phân tích song song."""
        target_regions = ["Vietnam", "Japan", "Singapore", "USA", "Germany"]
        logger.info("Khởi tạo danh sách thị trường phân tán: %s", target_regions)
        return target_regions

    @task(task_id="process_region_data")
    def process_region_data(region: str) -> Dict[str, Any]:
        """Task này sẽ tự động sinh task instance riêng biệt tương ứng với từng khu vực."""
        simulated_sales = 10_000 if region == "Vietnam" else 8_500
        logger.info("Xử lý hoàn tất thị trường: %s với doanh số %s", region, simulated_sales)
        return {"region": region, "status": "processed", "sales": simulated_sales}

    @task(task_id="aggregate_results")
    def aggregate_results(all_region_data: List[Dict[str, Any]]) -> int:
        """Gom nhóm kết quả từ toàn bộ các dynamic mapped instances."""
        total_sales = sum(item.get("sales", 0) for item in all_region_data)
        logger.info("Tổng hợp thành công %s thị trường. Tổng doanh số toàn cầu: %s", len(all_region_data), total_sales)
        return total_sales

    regions = get_source_regions()
    # .expand() nhân bản task process_region_data thành N task instances tương ứng
    mapped_results = process_region_data.expand(region=regions)
    total_sales_val = aggregate_results(mapped_results)

    # ------------------- PHẦN 2: BRANCHING LOGIC -------------------
    def choose_notification_channel(**context: Any) -> str:
        """Hàm phân nhánh quyết định task_id tiếp theo cần chạy dựa trên số liệu."""
        ti = context["ti"]
        total = ti.xcom_pull(task_ids="aggregate_results")
        logger.info("Branching evaluator nhận tổng doanh số: %s", total)

        # Nếu doanh số > 30,000 gửi thông báo qua Slack (High Priority), ngược lại gửi Email định kỳ
        if (total or 0) >= 30_000:
            logger.info("Doanh số vượt mốc mục tiêu! Chọn nhánh: send_slack_alert")
            return "send_slack_alert"
        logger.info("Doanh số chuẩn, chọn nhánh thông thường: send_email_alert")
        return "send_email_alert"

    branch_task = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=choose_notification_channel,
        doc_md="Phân nhánh quyết định gửi cảnh báo dựa vào tổng doanh thu.",
    )

    slack_task = EmptyOperator(
        task_id="send_slack_alert",
        doc_md="Gửi thông báo ưu tiên cao qua kênh Slack Webhook.",
    )

    email_task = EmptyOperator(
        task_id="send_email_alert",
        doc_md="Gửi báo cáo định kỳ qua máy chủ Email SMTP.",
    )

    # Điểm hội tụ sau phân nhánh: Yêu cầu TriggerRule phù hợp để không bị SKIPPED
    join_task = EmptyOperator(
        task_id="join_branches",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="Điểm hội tụ thành công khi ít nhất 1 nhánh hoàn tất và không có nhánh nào lỗi.",
    )

    # Thiết lập chuỗi thứ tự phụ thuộc
    total_sales_val >> branch_task
    branch_task >> [slack_task, email_task]
    [slack_task, email_task] >> join_task


dynamic_and_branching_pipeline()
