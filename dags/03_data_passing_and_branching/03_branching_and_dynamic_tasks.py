"""
==============================================================================
MODULE 03: PHÂN NHÁNH (BRANCHING) & DYNAMIC TASK MAPPING
==============================================================================
Mục tiêu bài học:
1. Sử dụng BranchPythonOperator để điều hướng luồng dữ liệu theo điều kiện động.
2. Thiết lập trigger_rule (như TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS) ở task hội tụ.
3. Sử dụng Dynamic Task Mapping (`@task.expand`) - tính năng mạnh mẽ của Airflow 2.3+.
"""

from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

@dag(
    dag_id="03_branching_and_dynamic_tasks",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["module_03", "branching", "dynamic_mapping"],
)
def dynamic_and_branching_pipeline():

    # ------------------- PHẦN 1: DYNAMIC TASK MAPPING -------------------
    @task
    def get_source_regions() -> list[str]:
        """Tạo danh sách các khu vực cần xử lý song song động."""
        return ["Vietnam", "Japan", "Singapore", "USA"]

    @task
    def process_region_data(region: str) -> dict:
        """Task này sẽ tự động nhân bản (expand) tương ứng với từng phần tử trong mảng."""
        print(f"Đang xử lý dữ liệu cho thị trường: {region}")
        return {"region": region, "status": "processed", "sales": 5000}

    @task
    def aggregate_results(all_region_data: list[dict]) -> int:
        """Gom nhóm kết quả từ các dynamic task instances."""
        total_sales = sum(item["sales"] for item in all_region_data)
        print(f"Tổng doanh thu toàn cầu: {total_sales}")
        return total_sales

    regions = get_source_regions()
    # .expand() tạo dynamic mapped tasks
    mapped_results = process_region_data.expand(region=regions)
    total = aggregate_results(mapped_results)

    # ------------------- PHẦN 2: BRANCHING LOGIC -------------------
    def choose_notification_channel(**kwargs):
        """Hàm phân nhánh quyết định task_id tiếp theo cần chạy."""
        # Giả lập logic kiểm tra: nếu ngày chẵn gửi Slack, ngày lẻ gửi Email
        day_of_month = datetime.now().day
        if day_of_month % 2 == 0:
            return "send_slack_alert"
        return "send_email_alert"

    branch_task = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=choose_notification_channel,
    )

    slack_task = EmptyOperator(task_id="send_slack_alert")
    email_task = EmptyOperator(task_id="send_email_alert")

    # Điểm hội tụ sau phân nhánh, yêu cầu trigger_rule phù hợp
    join_task = EmptyOperator(
        task_id="join_branches",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Thiết lập phụ thuộc
    total >> branch_task
    branch_task >> [slack_task, email_task]
    [slack_task, email_task] >> join_task

dynamic_and_branching_pipeline()
