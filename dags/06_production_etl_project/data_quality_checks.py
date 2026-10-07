"""
==============================================================================
MODULE 06: KIỂM ĐỊNH CHẤT LƯỢNG DỮ LIỆU (DATA QUALITY CHECKS)
==============================================================================
Mục tiêu bài học:
1. Thiết lập các chốt chặn chất lượng dữ liệu (Quality Gates) độc lập.
2. Kiểm tra các quy tắc cốt lõi: Tính không rỗng, tính không âm, tính duy nhất khóa chính.
3. Dừng luồng (Fail the DAG) ngay lập tức khi phát hiện dữ liệu bất thường (Anomalies).
4. Ngăn chặn triệt để dữ liệu lỗi rò rỉ vào Dashboard BI và người dùng cuối.
==============================================================================
"""

import logging
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow.decorators import dag, task

logger = logging.getLogger("airflow.task")

DB_PATH = os.path.join(tempfile.gettempdir(), "airflow_learning_warehouse.db")

default_args: Dict[str, Any] = {
    "owner": "data_platform_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

PIPELINE_DOC_MD = """
### 🛡️ DAG: Data Quality Validation Suite
Chốt chặn kiểm định chất lượng toàn diện trước khi phát hành dữ liệu cho BI:
- **`check_table_not_empty`**: Đảm bảo bảng đích đã được nạp dữ liệu.
- **`check_no_negative_amounts`**: Đảm bảo không có doanh thu âm bất thường.
- **`check_primary_key_uniqueness`**: Đảm bảo không bị trùng lặp khóa chính `order_id`.
- **`notify_data_readiness`**: Xác nhận toàn bộ bài kiểm tra đã PASSED và phát tín hiệu sẵn sàng.
"""


@dag(
    dag_id="data_quality_checks",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["module_06", "data_quality", "testing", "validation", "gatekeeper"],
    doc_md=PIPELINE_DOC_MD,
)
def data_quality_pipeline() -> None:

    @task(task_id="check_table_not_empty")
    def check_table_not_empty() -> int:
        """Kiểm tra bảng dữ liệu không được phép rỗng (Completeness Check)."""
        logger.info("Đang kiểm tra số lượng bản ghi trong database: %s", DB_PATH)
        if not os.path.exists(DB_PATH):
            raise FileNotFoundError(f"Database warehouse không tồn tại tại {DB_PATH}! Cần chạy ETL pipeline trước.")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM fact_orders;")
        row_count = cursor.fetchone()[0]
        conn.close()

        if row_count == 0:
            raise ValueError("🚨 [DATA QUALITY ERROR] Bảng fact_orders không chứa bản ghi nào!")

        logger.info("✅ PASS: Bảng fact_orders có %s dòng hợp lệ.", row_count)
        return row_count

    @task(task_id="check_no_negative_amounts")
    def check_no_negative_amounts() -> None:
        """Kiểm tra không có doanh thu âm trong bảng fact_orders (Validity Check)."""
        logger.info("Đang kiểm tra tính hợp lệ của doanh thu...")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM fact_orders WHERE amount_usd < 0 OR amount_vnd < 0;")
        invalid_rows = cursor.fetchone()[0]
        conn.close()

        if invalid_rows > 0:
            raise ValueError(f"🚨 [DATA QUALITY ERROR] Phát hiện {invalid_rows} dòng có doanh thu âm!")

        logger.info("✅ PASS: Tất cả đơn hàng đều có doanh thu hợp lệ (> 0).")

    @task(task_id="check_primary_key_uniqueness")
    def check_primary_key_uniqueness() -> None:
        """Kiểm tra không bị trùng lặp khóa chính order_id (Uniqueness Check)."""
        logger.info("Đang kiểm tra tính duy nhất của khóa chính order_id...")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT order_id, count(*)
            FROM fact_orders
            GROUP BY order_id
            HAVING count(*) > 1;
        """)
        duplicate_rows = cursor.fetchall()
        conn.close()

        if duplicate_rows:
            raise ValueError(f"🚨 [DATA QUALITY ERROR] Phát hiện trùng lặp khóa chính: {duplicate_rows}")

        logger.info("✅ PASS: Khóa chính order_id duy nhất 100%.")

    @task(task_id="notify_data_readiness")
    def notify_data_readiness() -> None:
        """Thông báo toàn bộ dữ liệu đã được xác thực an toàn và sẵn sàng cho BI."""
        logger.info("🎉 Toàn bộ Data Quality Checks đã vượt qua thành công! Dữ liệu sẵn sàng phục vụ Dashboard BI.")

    # Luồng kiểm tra song song các chốt chặn trước khi thông báo
    [check_table_not_empty(), check_no_negative_amounts(), check_primary_key_uniqueness()] >> notify_data_readiness()


data_quality_pipeline()
