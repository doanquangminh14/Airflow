"""
==============================================================================
MODULE 06: KIỂM ĐỊNH CHẤT LƯỢNG DỮ LIỆU (DATA QUALITY CHECKS)
==============================================================================
Mục tiêu bài học:
1. Thêm chốt chặn kiểm tra tính toàn vẹn (Data Validation / Assertion).
2. Dừng pipeline và raise Exception khi phát hiện dữ liệu lỗi hoặc bất thường (Anomalies).
3. Đảm bảo dữ liệu nạp vào Data Warehouse luôn chính xác 100%.
"""

from datetime import datetime
import sqlite3
from airflow.decorators import dag, task

DB_PATH = "/tmp/airflow_learning_warehouse.db"

@dag(
    dag_id="data_quality_checks",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["module_06", "data_quality", "testing", "validation"],
)
def data_quality_pipeline():

    @task
    def check_table_not_empty():
        """Kiểm tra bảng dữ liệu không được rỗng."""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM fact_orders;")
        row_count = cursor.fetchone()[0]
        conn.close()

        if row_count == 0:
            raise ValueError("🚨 [DATA QUALITY ERROR] Bảng fact_orders không có dữ liệu nào!")
        print(f"✅ Pass check: Bảng có {row_count} dòng.")

    @task
    def check_no_negative_amounts():
        """Kiểm tra không có doanh thu âm trong bảng fact_orders."""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM fact_orders WHERE amount_usd < 0 OR amount_vnd < 0;")
        invalid_rows = cursor.fetchone()[0]
        conn.close()

        if invalid_rows > 0:
            raise ValueError(f"🚨 [DATA QUALITY ERROR] Phát hiện {invalid_rows} dòng có doanh thu âm!")
        print("✅ Pass check: Tất cả đơn hàng đều có doanh thu hợp lệ (> 0).")

    @task
    def notify_data_readiness():
        """Thông báo toàn bộ dữ liệu đã được xác thực an toàn."""
        print("🎉 Toàn bộ Data Quality Checks đã vượt qua thành công! Dữ liệu sẵn sàng phục vụ Dashboard BI.")

    # Luồng kiểm tra
    [check_table_not_empty(), check_no_negative_amounts()] >> notify_data_readiness()

data_quality_pipeline()
