"""
==============================================================================
MODULE 06: DỰ ÁN ETL THỰC TẾ (PRODUCTION ETL PIPELINE)
==============================================================================
Mục tiêu bài học:
1. Xây dựng một luồng dữ liệu chuẩn (Extract -> Transform -> Load) theo phong cách TaskFlow API.
2. Xử lý chuẩn hóa dữ liệu, quy đổi tỷ giá tiền tệ, làm sạch và gắn metadata thời gian.
3. Tổ chức cấu trúc cơ sở dữ liệu SQLite Data Warehouse đa nền tảng (Windows/Linux).
4. Áp dụng Idempotency (UPSERT) và Transaction Management (Rollback an toàn khi có lỗi).
==============================================================================
"""

import logging
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow.decorators import dag, task

logger = logging.getLogger("airflow.task")

# Đường dẫn database độc lập nền tảng (chạy mượt mà trên cả Windows lẫn Linux/Docker)
DB_PATH = os.path.join(tempfile.gettempdir(), "airflow_learning_warehouse.db")

default_args: Dict[str, Any] = {
    "owner": "data_platform_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=15),
}

PIPELINE_DOC_MD = """
### 🏭 DAG: Production ETL Pipeline
Dự án ETL mẫu hoàn chỉnh xử lý dữ liệu đơn hàng:
1. **Extract**: Trích xuất đơn hàng từ nguồn bán lẻ (API / CRM payload).
2. **Transform**: Lọc bỏ các giao dịch thất bại / hoàn tiền, quy đổi doanh thu từ USD sang VND (tỷ giá cố định), làm giàu timestamp.
3. **Load**: Nạp dữ liệu vào bảng `fact_orders` của SQLite Data Warehouse sử dụng câu lệnh `INSERT OR REPLACE INTO` bảo đảm tính Idempotent.
"""


@dag(
    dag_id="etl_production_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["module_06", "etl", "production", "warehouse", "sqlite"],
    doc_md=PIPELINE_DOC_MD,
)
def etl_production_pipeline() -> None:

    @task(task_id="extract_orders_data")
    def extract_orders_data() -> List[Dict[str, Any]]:
        """Bước 1: Extract - Giả lập trích xuất danh sách đơn hàng từ REST API / CRM."""
        logger.info("Đang trích xuất dữ liệu đơn hàng từ nguồn API upstream...")
        orders_payload: List[Dict[str, Any]] = [
            {"order_id": "ORD-001", "customer": "Nguyen Van A", "amount": 250.0, "status": "COMPLETED", "currency": "USD"},
            {"order_id": "ORD-002", "customer": "Tran Thi B", "amount": 120.5, "status": "COMPLETED", "currency": "USD"},
            {"order_id": "ORD-003", "customer": "Le Van C", "amount": -10.0, "status": "REFUNDED", "currency": "USD"},
            {"order_id": "ORD-004", "customer": "Pham Thi D", "amount": 450.0, "status": "COMPLETED", "currency": "USD"},
            {"order_id": "ORD-005", "customer": "Doan Quang Minh", "amount": 890.0, "status": "COMPLETED", "currency": "USD"},
            {"order_id": "ORD-006", "customer": "Hoang Van E", "amount": 0.0, "status": "CANCELLED", "currency": "USD"},
        ]
        logger.info("Đã trích xuất thành công %s bản ghi từ API.", len(orders_payload))
        return orders_payload

    @task(task_id="transform_orders_data")
    def transform_orders_data(raw_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Bước 2: Transform - Làm sạch dữ liệu, quy đổi tỷ giá sang VND và lọc giao dịch hợp lệ."""
        logger.info("Đang tiến hành chuẩn hóa và làm giàu dữ liệu...")
        usd_to_vnd_rate = 25_400
        transformed_orders: List[Dict[str, Any]] = []

        for order in raw_orders:
            # Lọc chỉ lấy đơn COMPLETED có giá trị dương
            if order.get("status") == "COMPLETED" and order.get("amount", 0) > 0:
                amount_usd = float(order["amount"])
                amount_vnd = round(amount_usd * usd_to_vnd_rate, 2)
                transformed_orders.append({
                    "order_id": str(order["order_id"]),
                    "customer_name": str(order["customer"]),
                    "amount_usd": amount_usd,
                    "amount_vnd": amount_vnd,
                    "processed_at": datetime.utcnow().isoformat(),
                })

        logger.info(
            "Làm sạch hoàn tất: %s/%s đơn hàng đủ điều kiện nạp kho.",
            len(transformed_orders),
            len(raw_orders),
        )
        return transformed_orders

    @task(task_id="load_into_warehouse")
    def load_into_warehouse(clean_orders: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Bước 3: Load - Lưu dữ liệu vào Data Warehouse SQLite có Transaction Management."""
        logger.info("Nạp dữ liệu vào Database Warehouse tại: %s", DB_PATH)

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            # Khởi tạo bảng fact nếu chưa tồn tại
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fact_orders (
                    order_id TEXT PRIMARY KEY,
                    customer_name TEXT NOT NULL,
                    amount_usd REAL NOT NULL,
                    amount_vnd REAL NOT NULL,
                    processed_at TEXT NOT NULL
                )
            """)

            # Thao tác Upsert đảm bảo tính Idempotency khi chạy lại DAG
            for order in clean_orders:
                cursor.execute("""
                    INSERT OR REPLACE INTO fact_orders (
                        order_id, customer_name, amount_usd, amount_vnd, processed_at
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    order["order_id"],
                    order["customer_name"],
                    order["amount_usd"],
                    order["amount_vnd"],
                    order["processed_at"],
                ))

            conn.commit()
            logger.info("Đã commit thành công %s bản ghi vào bảng fact_orders.", len(clean_orders))
        except Exception as exc:
            conn.rollback()
            logger.error("Lỗi khi nạp dữ liệu! Đã Rollback giao dịch. Chi tiết: %s", exc)
            raise
        finally:
            conn.close()

        return {
            "status": "LOAD_COMPLETED",
            "inserted_rows": len(clean_orders),
            "db_path": DB_PATH,
        }

    # Thiết lập luồng phụ thuộc dữ liệu
    raw_payload = extract_orders_data()
    clean_payload = transform_orders_data(raw_payload)
    load_into_warehouse(clean_payload)


etl_production_pipeline()
