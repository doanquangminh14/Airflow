"""
==============================================================================
MODULE 01: CƠ BẢN VỀ AIRFLOW (CLASSIC DAG DEFINITION)
==============================================================================
Mục tiêu bài học:
1. Hiểu cấu trúc một DAG truyền thống trong Airflow.
2. Thiết lập default_args, start_date, schedule_interval, tags.
3. Tạo task với BashOperator và PythonOperator.
4. Định nghĩa thứ tự thực thi (Dependencies: >> và <<).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# 1. Cấu hình mặc định áp dụng cho tất cả các task trong DAG
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email": ["admin@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def print_welcome(execution_date=None, **context):
    """Hàm Python xử lý logic đơn giản và truy cập ngữ cảnh DAG."""
    print("==================================================")
    print("Chào mừng bạn đến với Apache Airflow!")
    print(f"Thời gian chạy Logical Date: {execution_date}")
    print("==================================================")
    return "Welcome message logged successfully"

# 2. Khởi tạo đối tượng DAG (sử dụng Context Manager 'with')
with DAG(
    dag_id="01_first_dag_classic",
    default_args=default_args,
    description="DAG cơ bản sử dụng cú pháp Classic Operator",
    schedule_interval="0 7 * * *",  # Chạy hàng ngày lúc 07:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Không chạy bù các lần trong quá khứ khi bật DAG lần đầu
    tags=["module_01", "basics", "classic_dag"],
) as dag:

    # 3. Tạo Task 1: BashOperator - Thực thi lệnh Shell
    task_start = BashOperator(
        task_id="task_start_bash",
        bash_command='echo "Airflow pipeline bắt đầu lúc $(date)"',
    )

    # 4. Tạo Task 2: PythonOperator - Thực thi hàm Python
    task_python_logic = PythonOperator(
        task_id="task_python_logic",
        python_callable=print_welcome,
        provide_context=True,
    )

    # 5. Tạo Task 3: BashOperator kết thúc
    task_end = BashOperator(
        task_id="task_end_bash",
        bash_command='echo "Airflow pipeline hoàn thành thành công!"',
    )

    # 6. Thiết lập thứ tự chạy: task_start -> task_python_logic -> task_end
    task_start >> task_python_logic >> task_end
