# 🔌 Module Plugins: Airflow Plugin Architecture

Thư mục `plugins/` là nơi chứa các thành phần mở rộng tùy chỉnh cho Apache Airflow như **Custom Operators**, **Custom Hooks**, **Custom Sensors**, **Macros**, và **UI Views/Menus**.

---

## 🎯 Mục Tiêu Học Tập
1. Hiểu cơ chế nạp plugin tự động của Airflow từ thư mục `plugins/`.
2. Xây dựng **Custom Hook** kế thừa từ `BaseHook` để đóng gói logic kết nối hoặc ghi log kiểm toán.
3. Xây dựng **Custom Operator** kế thừa từ `BaseOperator` có template fields.
4. Đóng gói plugin với lớp `AirflowPlugin` để Airflow tự động nhận diện trong toàn bộ hệ thống.

---

## 📂 Danh Sách Files & Chi Tiết Triển Khai

| File | Mô Tả & Khái Niệm Chính |
| :--- | :--- |
| [`custom_plugins.py`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/plugins/custom_plugins.py) | Định nghĩa `CustomAuditHook` (ghi log kiểm toán), `CustomGreetingOperator` (xử lý lời chào & đếm từ) và `CustomLearningPlugin` kế thừa từ `AirflowPlugin`. |

---

## 🧩 Cấu Trúc Đóng Gói Plugin

```python
from airflow.plugins_manager import AirflowPlugin
from custom_plugins import CustomGreetingOperator, CustomAuditHook

class CustomLearningPlugin(AirflowPlugin):
    name = "custom_learning_plugin"
    operators = [CustomGreetingOperator]
    hooks = [CustomAuditHook]
```

---

## 🚀 Cách Sử Dụng Trong DAG

```python
from airflow import DAG
from datetime import datetime
from custom_plugins import CustomGreetingOperator

with DAG(dag_id="test_plugin_dag", start_date=datetime(2024, 1, 1), schedule=None) as dag:
    greet = CustomGreetingOperator(
        task_id="greet_user",
        recipient_name="Data Engineer",
        message="Xin chào",
    )
```
