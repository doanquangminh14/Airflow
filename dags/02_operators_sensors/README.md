# 📌 Module 02: Operators, Sensors & Custom Extensions

Module này tập trung vào các khối xây dựng cốt lõi (Building Blocks) của Airflow: **Operators chuẩn**, **Sensors theo hướng sự kiện (Event-Driven)** và **Custom Operator tự định nghĩa**.

---

## 🎯 Mục Tiêu Học Tập
1. Sử dụng thành thạo các Operator chuẩn: `BashOperator`, `PythonOperator`, `EmptyOperator`.
2. Ứng dụng **Jinja Templating** và **Airflow Macros** (`{{ ds }}`, `{{ dag.dag_id }}`) để động hóa cấu hình pipeline.
3. Hiểu cơ chế hoạt động của **Sensors** và sự khác biệt cốt lõi giữa `mode="poke"` vs `mode="reschedule"`.
4. Tự viết **Custom Operator** kế thừa từ `BaseOperator` có hỗ trợ `template_fields` để tái sử dụng trong toàn bộ hệ thống.

---

## 📂 Danh Sách Files & Chi Tiết Triển Khai

| File | Mô Tả & Khái Niệm Chính |
| :--- | :--- |
| [`01_standard_operators.py`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/dags/02_operators_sensors/01_standard_operators.py) | Trình diễn sử dụng `EmptyOperator` làm anchor point, `BashOperator` truyền biến môi trường và Jinja template, `PythonOperator` truyền tham số qua `op_kwargs`. |
| [`02_sensors_demo.py`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/dags/02_operators_sensors/02_sensors_demo.py) | Mô hình Event-driven với `PythonSensor`. Thiết lập `poke_interval`, `timeout` và chế độ tối ưu worker `mode="reschedule"` (giải phóng slot worker trong lúc chờ). |
| [`03_custom_operator_hook.py`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/dags/02_operators_sensors/03_custom_operator_hook.py) | Xây dựng class `CleanAndValidateCsvOperator(BaseOperator)` với `template_fields = ("source_name",)` kiểm tra dữ liệu và trả kết quả về XCom. |

---

## 💡 Sensor Execution Modes: `poke` vs `reschedule`

```
Mode: 'poke' (Mặc định)
[Worker Slot] ────────▶ [Đang chờ file/API] ────────▶ [Chiếm giữ worker slot liên tục, gây nghẽn pool]

Mode: 'reschedule' (Best Practice)
[Worker Slot] ──(Check 1)──▶ [Ngủ & Giải phóng Slot] ──(Check 2)──▶ [Thành công & Nhận slot xử lý]
```

- **`poke`**: Phù hợp khi thời gian chờ cực ngắn (< 1 phút).
- **`reschedule`**: Bắt buộc dùng khi thời gian chờ lâu (> vài phút) để tránh làm cạn kiệt Worker Slots của cluster.

---

## 🚀 Hướng Dẫn Kiểm Thử & Thực Thi

### 1. Kiểm tra cú pháp DAG
```bash
python dags/02_operators_sensors/01_standard_operators.py
python dags/02_operators_sensors/02_sensors_demo.py
python dags/02_operators_sensors/03_custom_operator_hook.py
```

### 2. Test Custom Operator trong CLI
```bash
airflow tasks test 03_custom_operator_hook validate_sales_data 2024-01-01
```
