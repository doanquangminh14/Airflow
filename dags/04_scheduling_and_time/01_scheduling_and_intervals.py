"""
==============================================================================
MODULE 04: LẬP LỊCH & KHOẢNG THỜI GIAN (SCHEDULING & DATA INTERVALS)
==============================================================================
Mục tiêu bài học:
1. Nắm vững bản chất thời gian: Logical Date (Execution Date) vs Data Interval Start/End.
2. Thiết lập Timezone nhận biết chính xác (Timezone-Aware) bằng thư viện Pendulum.
3. Hiểu tại sao DAG chạy lúc `data_interval_end` (sau khi khoảng dữ liệu đã trôi qua hoàn toàn).
4. Sử dụng thành thạo các biến ngữ cảnh Jinja: `data_interval_start`, `data_interval_end`, `ds`, `ts`.
==============================================================================
"""

import logging
from datetime import timedelta
from typing import Any, Dict

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

# Khởi tạo múi giờ Việt Nam (UTC+7) chuẩn Data Engineering
LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

DAG_DOC_MD = """
### ⏱️ DAG: Scheduling & Data Intervals Deep Dive
Phân tích chi tiết mô hình lập lịch và khoảng dữ liệu của Airflow:
- **`data_interval_start`**: Mốc bắt đầu của khoảng dữ liệu cần trích xuất (bao gồm cả mốc này).
- **`data_interval_end`**: Mốc kết thúc của khoảng dữ liệu. **Đây là thời điểm sớm nhất Scheduler kích hoạt DAG!**
- **`logical_date`**: Tương đương `data_interval_start` trong các DAG chạy định kỳ tiêu chuẩn.
- **Timezone Awareness**: DAG được gắn cố định với múi giờ `Asia/Ho_Chi_Minh` để tránh nhầm lẫn giờ mùa hè (DST).
"""


def inspect_data_intervals(**context: Any) -> Dict[str, str]:
    """Phân tích chi tiết và ghi log các mốc thời gian của DagRun hiện tại."""
    logical_date = context.get("logical_date")
    data_interval_start = context.get("data_interval_start")
    data_interval_end = context.get("data_interval_end")
    ds = context.get("ds", "N/A")
    ts = context.get("ts", "N/A")

    logger.info("=" * 60)
    logger.info("KHOẢNG THỜI GIAN DỮ LIỆU CỦA DAG RUN (DATA INTERVALS)")
    logger.info("1. Logical Date (Execution Date) : %s", logical_date)
    logger.info("2. Data Interval Start           : %s", data_interval_start)
    logger.info("3. Data Interval End (Trigger At): %s", data_interval_end)
    logger.info("4. Ngày xử lý (ds)               : %s", ds)
    logger.info("5. Timestamp ISO (ts)            : %s", ts)
    logger.info("=" * 60)
    logger.info("QUY TẮC BẤT BIẾN: Pipeline chỉ chạy sau khi 'data_interval_end' đã kết thúc!")

    return {
        "logical_date": str(logical_date),
        "data_interval_start": str(data_interval_start),
        "data_interval_end": str(data_interval_end),
        "ds": ds,
    }


default_args: Dict[str, Any] = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="01_scheduling_and_intervals",
    default_args=default_args,
    # Khởi tạo ngày bắt đầu có gắn múi giờ rõ ràng
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    # Chạy mỗi giờ vào phút thứ 0: Chu kỳ [01:00 -> 02:00] sẽ kích hoạt lúc 02:00
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["module_04", "scheduling", "intervals", "timezone"],
    doc_md=DAG_DOC_MD,
) as dag:

    explain_time_task = PythonOperator(
        task_id="inspect_time_variables",
        python_callable=inspect_data_intervals,
        provide_context=True,
        doc_md="Truy xuất và ghi log chi tiết các biến thời gian của DagRun context.",
    )
