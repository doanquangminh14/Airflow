"""
==============================================================================
MODULE 03: XCOMS TRONG AIRFLOW (CROSS-COMMUNICATION)
==============================================================================
Mục tiêu bài học:
1. Hiểu cơ chế XCom: cách trao đổi metadata/small payloads giữa các task.
2. Sử dụng `ti.xcom_push` và `ti.xcom_pull` với key tùy chỉnh.
3. Cảnh báo quan trọng: KHÔNG dùng XCom để truyền big data (DataFrames lớn, file dung lượng lớn).
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def push_metrics_data(**context):
    """Đẩy nhiều dữ liệu vào XCom qua TaskInstance."""
    ti = context["ti"]
    
    # 1. Trả về giá trị mặc định (tương đương xcom_push với key='return_value')
    # 2. Đẩy thêm dữ liệu có gắn key cụ thể
    ti.xcom_push(key="model_accuracy", value=0.954)
    ti.xcom_push(key="processed_record_count", value=15420)
    
    print("Đã đẩy thành công các metrics vào XCom Database.")
    return {"status": "SUCCESS", "version": "v1.2.0"}

def pull_metrics_data(**context):
    """Kéo dữ liệu từ các task trước đó thông qua key."""
    ti = context["ti"]
    
    # Lấy giá trị return_value mặc định
    main_result = ti.xcom_pull(task_ids="producer_task", key="return_value")
    # Lấy các giá trị theo custom key
    accuracy = ti.xcom_pull(task_ids="producer_task", key="model_accuracy")
    records = ti.xcom_pull(task_ids="producer_task", key="processed_record_count")
    
    print("================ KẾT QUẢ TỪ XCOM ================")
    print(f"Trạng thái: {main_result}")
    print(f"Độ chính xác mô hình: {accuracy * 100:.2f}%")
    print(f"Tổng số bản ghi: {records}")
    print("=================================================")

with DAG(
    dag_id="01_xcom_data_sharing",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["module_03", "xcom", "data_sharing"],
) as dag:

    producer = PythonOperator(
        task_id="producer_task",
        python_callable=push_metrics_data,
    )

    consumer = PythonOperator(
        task_id="consumer_task",
        python_callable=pull_metrics_data,
    )

    producer >> consumer
