"""
==============================================================================
MODULE 06: DỰ ÁN ETL THỰC TẾ (PRODUCTION ETL PIPELINE)
==============================================================================
Mục tiêu bài học:
1. Xây dựng một luồng dữ liệu chuẩn (Extract -> Transform -> Load).
2. Xử lý dữ liệu thực tế bằng Pandas và nạp vào SQLite Database.
3. Tổ chức cấu trúc code chuẩn production, log rõ ràng.
"""

from datetime import datetime, timedelta
import os
import json
import sqlite3
from airflow.decorators import dag, task

DB_PATH = "/tmp/airflow_learning_warehouse.db"

default_args = {
    "owner": "data_platform_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="etl_production_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["module_06", "etl", "production", "pandas", "sqlite"],
)
def etl_production_pipeline():

    @task
    def extract_orders_data() -> list[dict]:
        """
        Bước 1: Extract - Giả lập trích xuất danh sách đơn hàng từ REST API / CRM.
        """
        print("Đang trích xuất dữ liệu đơn hàng từ nguồn API...")
        orders_payload = [
            {"order_id": "ORD-001", "customer": "Nguyen Van A", "amount": 250.0, "status": "COMPLETED", "currency": "USD"},
            {"order_id": "ORD-002", "customer": "Tran Thi B", "amount": 120.5, "status": "COMPLETED", "currency": "USD"},
            {"order_id": "ORD-003", "customer": "Le Van C", "amount": -10.0, "status": "REFUNDED", "currency": "USD"},
            {"order_id": "ORD-004", "customer": "Pham Thi D", "amount": 450.0, "status": "COMPLETED", "currency": "USD"},
            {"order_id": "ORD-005", "customer": "Doan Quang Minh", "amount": 890.0, "status": "COMPLETED", "currency": "USD"},
        ]
        print(f"Đã trích xuất {len(orders_payload)} bản ghi.")
        return orders_payload

    @task
    def transform_orders_data(raw_orders: list[dict]) -> list[dict]:
        """
        Bước 2: Transform - Làm sạch dữ liệu, quy đổi tỷ giá sang VND, lọc đơn hoàn tiền.
        """
        print("Đang tiến hành chuẩn hóa và làm giàu dữ liệu...")
        USD_TO_VND_RATE = 25400
        transformed_orders = []

        for order in raw_orders:
            # Lọc chỉ lấy đơn hoàn thành hợp lệ
            if order["status"] == "COMPLETED" and order["amount"] > 0:
                amount_vnd = round(order["amount"] * USD_TO_VND_RATE, 2)
                transformed_orders.append({
                    "order_id": order["order_id"],
                    "customer_name": order["customer"],
                    "amount_usd": order["amount"],
                    "amount_vnd": amount_vnd,
                    "processed_at": datetime.utcnow().isoformat(),
                })

        print(f"Làm sạch hoàn tất: {len(transformed_orders)}/{len(raw_orders)} đơn hàng hợp lệ.")
        return transformed_orders

    @task
    def load_into_warehouse(clean_orders: list[dict]) -> str:
        """
        Bước 3: Load - Lưu dữ liệu vào cơ sở dữ liệu SQLite data warehouse.
        """
        print(f"Nạp dữ liệu vào Database: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_orders (
                order_id TEXT PRIMARY KEY,
                customer_name TEXT,
                amount_usd REAL,
                amount_vnd REAL,
                processed_at TEXT
            )
        """)

        for order in clean_orders:
            cursor.execute("""
                INSERT OR REPLACE INTO fact_orders (order_id, customer_name, amount_usd, amount_vnd, processed_at)
                VALUES (?, ?, ?, ?, ?)
            """, (
                order["order_id"],
                order["customer_name"],
                order["amount_usd"],
                order["amount_vnd"],
                order["processed_at"]
            ))

        conn.commit()
        conn.close()
        return f"Đã lưu thành công {len(clean_orders)} dòng vào bảng fact_orders."

    # Tạo luồng dữ liệu
    raw_data = extract_orders_data()
    clean_data = transform_orders_data(raw_data)
    load_into_warehouse(clean_data)

etl_production_pipeline()
