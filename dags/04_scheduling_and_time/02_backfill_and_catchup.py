"""
==============================================================================
MODULE 04: CATCHUP, BACKFILL & PHỤ THUỘC QUÁ KHỨ (DEPENDS_ON_PAST)
==============================================================================
Mục tiêu bài học:
1. Ý nghĩa tham số `catchup=True` vs `catchup=False` và cách phòng ngừa Task Storm.
2. Tham số `max_active_runs`: giới hạn số DagRun chạy đồng thời để bảo vệ tài nguyên Database.
3. Tham số `depends_on_past=True`: đảm bảo tính toàn vẹn dữ liệu chuỗi thời gian (Time-series).
4. Thiết kế task có tính lũy đạo (Idempotency) để chạy lại bất kỳ ngày nào mà không sợ nhân đôi dữ liệu.
==============================================================================
"""

import logging
from datetime import timedelta
from typing import Any, Dict

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

DAG_DOC_MD = """
### ⏪ DAG: Backfill, Catchup & Sequential Dependencies
Thực hành các cấu hình an toàn khi xử lý dữ liệu lịch sử:
- **`catchup=False`**: Ngăn chặn tình trạng Airflow tự động kích hoạt hàng trăm run cũ khi vừa bật DAG.
- **`max_active_runs=1`**: Giới hạn tối đa 1 instance chạy tại một thời điểm, tránh cạnh tranh khóa dữ liệu (Deadlock).
- **`depends_on_past=True`**: Task của ngày $N$ chỉ được phép thực thi nếu cùng task đó ở ngày $N-1$ đã hoàn thành thành công.
"""


def sequential_daily_process(**context: Any) -> None:
    """Xử lý phân vùng dữ liệu ngày theo thứ tự tuần tự nghiêm ngặt (Idempotent processing)."""
    ds = context.get("ds", "N/A")
    data_start = context.get("data_interval_start")
    data_end = context.get("data_interval_end")

    logger.info("=" * 60)
    logger.info("BẮT ĐẦU XỬ LÝ PHÂN VÙNG DỮ LIỆU LỊCH SỬ")
    logger.info("Phân vùng ngày (ds): %s", ds)
    logger.info("Khoảng thời gian: [%s] -> [%s]", data_start, data_end)
    logger.info("Thực thi câu lệnh SQL dạng: DELETE FROM target WHERE date='%s'; INSERT INTO target ...", ds)
    logger.info("Đảm bảo tính Idempotent: Chạy lại 100 lần kết quả vẫn nhất quán và không lỗi!")
    logger.info("=" * 60)


default_args: Dict[str, Any] = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # depends_on_past=True đảm bảo thứ tự thời gian không bị gián đoạn
    "depends_on_past": True,
}

with DAG(
    dag_id="02_backfill_and_catchup",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    schedule_interval="@daily",
    # catchup=False: Chỉ chạy lần gần nhất, không tự động sinh bão tasks cũ
    catchup=False,
    # max_active_runs=1: Đảm bảo chỉ 1 ngày được xử lý tại một thời điểm
    max_active_runs=1,
    tags=["module_04", "catchup", "backfill", "depends_on_past"],
    doc_md=DAG_DOC_MD,
) as dag:

    daily_task = PythonOperator(
        task_id="process_historical_partition",
        python_callable=sequential_daily_process,
        provide_context=True,
        doc_md="Xử lý dữ liệu phân vùng ngày có bảo vệ phụ thuộc chuỗi lịch sử.",
    )
