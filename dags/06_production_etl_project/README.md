# 📌 Module 06: Production ETL Project & Data Quality Validation

Module này kết hợp toàn bộ các kỹ thuật đã học để xây dựng một hệ thống dữ liệu hoàn chỉnh đạt chuẩn Production: từ trích xuất, làm sạch, biến đổi dữ liệu, nạp vào Data Warehouse (SQLite) cho đến các chốt chặn kiểm định chất lượng dữ liệu (**Data Quality Checks**).

---

## 🎯 Mục Tiêu Học Tập
1. Xây dựng Data Pipeline hoàn chỉnh theo kiến trúc chuẩn **Extract -> Transform -> Load (ETL)**.
2. Xử lý làm sạch, chuẩn hóa kiểu dữ liệu, quy đổi tỷ giá ngoại tệ và lọc đơn hoàn tiền.
3. Thiết kế bảng Data Warehouse có tính chất **Idempotency (Tính bất biến/Nhập dữ liệu không trùng lặp)** bằng `INSERT OR REPLACE INTO`.
4. Thiết lập hệ thống **Data Quality Validation** độc lập để ngăn chặn dữ liệu lỗi rò rỉ vào các Dashboard BI/Analytics.

---

## 📂 Danh Sách Files & Chi Tiết Triển Khai

| File | Mô Tả & Khái Niệm Chính |
| :--- | :--- |
| [`etl_production_pipeline.py`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/dags/06_production_etl_project/etl_production_pipeline.py) | Pipeline chính: Trích xuất đơn hàng giả lập từ API -> Chuyển đổi tiền tệ USD sang VND -> Nạp vào bảng `fact_orders` trong SQLite Warehouse (`/tmp/airflow_learning_warehouse.db`). |
| [`data_quality_checks.py`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/dags/06_production_etl_project/data_quality_checks.py) | Pipeline kiểm định chất lượng: Kiểm tra bảng `fact_orders` không được rỗng (`check_table_not_empty`) và không có doanh thu âm (`check_no_negative_amounts`). |

---

## 🏗️ Luồng Xử Lý Kiến Trúc (Architecture Pipeline)

```
[REST API/CRM] 
       │ (extract_orders_data)
       ▼
[Raw JSON Records] 
       │ (transform_orders_data: Clean, USD->VND, Filter Status)
       ▼
[Cleaned Fact Data] 
       │ (load_into_warehouse: Idempotent Insert)
       ▼
[(DB) SQLite Data Warehouse: fact_orders]
       │
       ├─────────────────────────────────────────┐
       ▼                                         ▼
[Check: Row Count > 0]                 [Check: Amounts >= 0]
       └────────────────────┬────────────────────┘
                            ▼
               [notify_data_readiness]
                            ▼
              [🚀 Ready for BI Dashboards]
```

---

## 🚀 Hướng Dẫn Kiểm Thử & Thực Thi

### 1. Kiểm tra cú pháp DAG
```bash
python dags/06_production_etl_project/etl_production_pipeline.py
python dags/06_production_etl_project/data_quality_checks.py
```

### 2. Kích hoạt và kiểm tra kết quả trong SQLite
```bash
# Kích hoạt chạy DAG ETL
airflow dags trigger etl_production_pipeline

# Kích hoạt chạy DAG kiểm định chất lượng
airflow dags trigger data_quality_checks
```
