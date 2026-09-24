# 📌 Module 01: Airflow Basics & DAG Definition

Module này cung cấp nền tảng kiến thức nhập môn về **Apache Airflow**, tập trung vào cách định nghĩa pipeline dữ liệu (DAG - Directed Acyclic Graph) thông qua hai phong cách: **Classic DAG** (Operator truyền thống) và **Modern TaskFlow API** (Airflow 2.x+).

---

## 🎯 Mục Tiêu Học Tập
1. Hiểu kiến trúc cốt lõi và chu trình thực thi của một DAG trong Airflow.
2. Nắm vững cách cấu hình tham số cơ bản: `default_args`, `start_date`, `schedule_interval`, `catchup`, `tags`.
3. Biết cách sử dụng các Operator cơ bản: `BashOperator`, `PythonOperator`.
4. Thiết lập quan hệ phụ thuộc (Task Dependencies) qua toán tử Bitshift (`>>`, `<<`).
5. Tiếp cận **TaskFlow API** (`@dag`, `@task`) để code ngắn gọn, tự động truyền dữ liệu và dễ bảo trì.

---

## 📂 Danh Sách Files & Chi Tiết Triển Khai

| File | Mô Tả & Khái Niệm Chính |
| :--- | :--- |
| [`01_first_dag_classic.py`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/dags/01_basics/01_first_dag_classic.py) | Định nghĩa DAG kiểu truyền thống với Context Manager `with DAG(...) as dag:`, sử dụng `BashOperator`, `PythonOperator` và toán tử `task_start >> task_python_logic >> task_end`. |
| [`02_taskflow_modern_dag.py`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/dags/01_basics/02_taskflow_modern_dag.py) | Định nghĩa pipeline theo TaskFlow API với Decorator `@dag` và `@task`. Dữ liệu được trả về và truyền vào như hàm Python thông thường, XComs được tự động quản lý. |

---

## ⚖️ So Sánh: Classic DAG vs. TaskFlow API

| Tiêu Chí | Classic DAG (`01_first_dag_classic.py`) | TaskFlow API (`02_taskflow_modern_dag.py`) |
| :--- | :--- | :--- |
| **Phiên bản hỗ trợ** | Airflow 1.x & 2.x | Airflow 2.0+ |
| **Cú pháp định nghĩa** | Class-based (`PythonOperator`, `BashOperator`) | Decorator (`@task`, `@dag`) |
| **Truyền nhận dữ liệu** | Thủ công qua `xcom_push` / `xcom_pull` | Tự động truyền qua tham số/giá trị trả về của hàm |
| **Độ phức tạp code** | Nhiều boilerplate code | Ngắn gọn, chuẩn Pythonic |
| **Trường hợp khuyên dùng** | Khi tích hợp các Service bên ngoài (S3, Spark, Postgres) | Khi viết logic xử lý Python nội bộ (ETL/ELT) |

---

## 🚀 Hướng Dẫn Kiểm Thử & Thực Thi

### 1. Kiểm tra cú pháp DAG (Syntax Check)
```bash
python dags/01_basics/01_first_dag_classic.py
python dags/01_basics/02_taskflow_modern_dag.py
```

### 2. Liệt kê các DAG và Task trong CLI
```bash
airflow dags list | grep 01_
airflow tasks list 01_first_dag_classic
airflow tasks list 02_taskflow_modern_dag
```

### 3. Chạy thử một Task đơn lẻ (Dry Run / Test mode)
```bash
# Test task Python trong DAG classic
airflow tasks test 01_first_dag_classic task_python_logic 2024-01-01

# Test task Extract trong TaskFlow DAG
airflow tasks test 02_taskflow_modern_dag extract_user_data 2024-01-01
```
