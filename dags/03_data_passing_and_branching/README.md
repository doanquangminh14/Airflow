# 📌 Module 03: Data Passing, Variables & Dynamic Branching

Module này bao quát các kỹ thuật truyền nhận dữ liệu giữa các task, quản trị biến cấu hình/kết nối bảo mật, cùng các cơ chế điều hướng luồng động: **XComs**, **Variables & Connections**, **Branching** và **Dynamic Task Mapping**.

---

## 🎯 Mục Tiêu Học Tập
1. Hiểu cơ chế **XCom (Cross-Communication)**: cách trao đổi metadata/small payloads an toàn và tránh anti-pattern truyền dữ liệu lớn (Big Data).
2. Quản lý biến môi trường với **Airflow Variables** và kết nối bí mật với **Airflow Connections** đúng chuẩn performance (tránh Top-Level Code SQL queries).
3. Triển khai phân nhánh logic động với **`BranchPythonOperator`** và xử lý điểm hội tụ bằng **`TriggerRule`** phù hợp.
4. Tận dụng sức mạnh của **Dynamic Task Mapping (`@task.expand`)** trong Airflow 2.3+ để xử lý dữ liệu song song theo danh sách phần tử động.

---

## 📂 Danh Sách Files & Chi Tiết Triển Khai

| File | Mô Tả & Khái Niệm Chính |
| :--- | :--- |
| [`01_xcom_data_sharing.py`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/dags/03_data_passing_and_branching/01_xcom_data_sharing.py) | Đẩy và kéo dữ liệu qua `ti.xcom_push(key, value)` và `ti.xcom_pull(task_ids, key)`. Thể hiện mô hình Producer -> Consumer cho metadata & metrics. |
| [`02_variables_connections.py`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/dags/03_data_passing_and_branching/02_variables_connections.py) | Đọc `Variable.get()` an toàn trong runtime của task và lấy thông tin connection qua `BaseHook.get_connection()`. |
| [`03_branching_and_dynamic_tasks.py`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/dags/03_data_passing_and_branching/03_branching_and_dynamic_tasks.py) | Dynamic Task Mapping qua `.expand()` để xử lý song song các vùng dữ liệu (Vietnam, Japan, Singapore, USA) kết hợp với `BranchPythonOperator` và `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS`. |

---

## ⚠️ Quy Tắc Vàng Khi Dùng XCom & Variables

| Thành Phần | Khuyên Dùng (Do's) | Cấm Kỵ (Don'ts) |
| :--- | :--- | :--- |
| **XCom** | Lưu ID, đường dẫn file S3/GCS/HDFS, số lượng dòng, trạng thái execution | Lưu nguyên DataFrame Pandas, File nhị phân, dữ liệu > 48KB |
| **Variables** | Gọi bên trong hàm Python callable / execute method | Gọi ở Top-level script (gây overload database mỗi khi scheduler parse DAG) |
| **Branching** | Thiết lập `trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` cho task hội tụ sau nhánh | Dùng mặc định `all_success` ở node hội tụ vì các nhánh bị skip sẽ làm node sau fail |

---

## 🚀 Hướng Dẫn Kiểm Thử & Thực Thi

### 1. Kiểm tra cú pháp DAG
```bash
python dags/03_data_passing_and_branching/01_xcom_data_sharing.py
python dags/03_data_passing_and_branching/02_variables_connections.py
python dags/03_data_passing_and_branching/03_branching_and_dynamic_tasks.py
```

### 2. Thiết lập Variable thử nghiệm qua CLI
```bash
airflow variables set APP_ENVIRONMENT "production"
airflow variables set API_RATE_LIMIT "250"
```
