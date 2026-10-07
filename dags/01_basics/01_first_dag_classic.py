"""
==============================================================================
MODULE 01: CƠ BẢN VỀ AIRFLOW (CLASSIC DAG DEFINITION)
==============================================================================
Mục tiêu bài học:
1. Hiểu cấu trúc và chu trình khởi tạo một DAG truyền thống trong Apache Airflow.
2. Thiết lập default_args, start_date, schedule_interval, catchup, tags và doc_md.
3. Tạo task với BashOperator và PythonOperator.
4. Định nghĩa thứ tự thực thi (Task Dependencies: >> và <<).
5. Truy cập ngữ cảnh Airflow Context (execution_date, logical_date, ti, params).
==============================================================================
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Khởi tạo logger theo chuẩn Airflow Task Logger
logger = logging.getLogger("airflow.task")

# 1. Cấu hình mặc định (default_args) áp dụng chung cho tất cả các tasks trong DAG
default_args: Dict[str, Any] = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email": ["admin@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=15),
}

DAG_DOC_MD = """
### 🚀 DAG 01: First Classic DAG
Pipeline giới thiệu cấu trúc DAG truyền thống của Apache Airflow:
- **Task Start (Bash)**: Ghi log thời gian bắt đầu hệ thống.
- **Task Python Logic**: Truy xuất context, tính toán metadata và in thông tin thực thi.
- **Task End (Bash)**: Xác nhận luồng pipeline hoàn thành thành công.
"""


def process_welcome_logic(execution_date: Any = None, **context: Any) -> str:
    """Hàm Python xử lý logic nghiệp vụ và truy cập ngữ cảnh DAG.

    Args:
        execution_date: Logical date của lần chạy DAG.
        **context: Toàn bộ từ điển Airflow Task Instance context.

    Returns:
        str: Chuỗi thông báo trạng thái hoàn tất task.
    """
    ti = context.get("ti")
    dag_run = context.get("dag_run")
    logical_date = context.get("logical_date", execution_date)

    logger.info("=" * 60)
    logger.info("Khởi chạy task logic với PythonOperator thành công!")
    logger.info("Logical Date: %s", logical_date)
    logger.info("DAG ID: %s | Task ID: %s", ti.dag_id if ti else "N/A", ti.task_id if ti else "N/A")
    logger.info("DAG Run ID: %s", dag_run.run_id if dag_run else "Manual")
    logger.info("=" * 60)

    return f"Processed successfully for logical_date={logical_date}"


# 2. Khởi tạo đối tượng DAG sử dụng Context Manager 'with'
with DAG(
    dag_id="01_first_dag_classic",
    default_args=default_args,
    description="DAG cơ bản sử dụng cú pháp Classic Operator (Bash & Python)",
    schedule_interval="0 7 * * *",  # Chạy định kỳ lúc 07:00 UTC hàng ngày
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Ngăn chặn việc chạy bù ồ ạt các mốc thời gian quá khứ
    max_active_runs=1,  # Đảm bảo chỉ 1 DAG run chạy tại một thời điểm
    tags=["module_01", "basics", "classic_dag"],
    doc_md=DAG_DOC_MD,
) as dag:

    # 3. Task 1: BashOperator - Thực thi lệnh hệ điều hành Linux/Bash
    task_start = BashOperator(
        task_id="task_start_bash",
        bash_command='echo "Airflow pipeline bắt đầu lúc $(date)"',
        doc_md="Khởi động pipeline và in thời gian hệ thống.",
    )

    # 4. Task 2: PythonOperator - Thực thi hàm Python nghiệp vụ với context
    task_python_logic = PythonOperator(
        task_id="task_python_logic",
        python_callable=process_welcome_logic,
        provide_context=True,
        doc_md="Xử lý nghiệp vụ Python, đọc ngữ cảnh task instance và ghi log.",
    )

    # 5. Task 3: BashOperator - Task kết thúc thông báo hoàn thành
    task_end = BashOperator(
        task_id="task_end_bash",
        bash_command='echo "Airflow pipeline hoàn thành thành công!"',
        doc_md="Ghi nhận pipeline đã hoàn tất toàn bộ chu trình.",
    )

    # 6. Thiết lập quan hệ phụ thuộc (Dependencies: A >> B >> C)
    task_start >> task_python_logic >> task_end
