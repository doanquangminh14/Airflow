# 📌 Module 05: Monitoring, Resilience & Task Groups

Module này cung cấp các phương pháp chuẩn công nghiệp để xây dựng pipeline có khả năng chống chịu lỗi cao (Fault-tolerant), tích hợp cơ chế cảnh báo chủ động (Alerting) và cấu trúc giao diện DAG rõ ràng với **Task Groups**.

---

## 🎯 Mục Tiêu Học Tập
1. Thiết lập cơ chế tự phục hồi lỗi với `retries`, `retry_delay`, `retry_exponential_backoff`, và `max_retry_delay`.
2. Xây dựng hệ thống Callback thông minh: `on_failure_callback`, `on_success_callback`, `on_retry_callback` (dùng để gửi webhook Telegram/Slack/Email).
3. Sử dụng **TaskGroup** để nhóm các bước xử lý liên quan (Extract, Transform, Load) giúp UI trực quan, tránh lỗi deadlock của mô hình SubDAG cũ.

---

## 📂 Danh Sách Files & Chi Tiết Triển Khai

| File | Mô Tả & Khái Niệm Chính |
| :--- | :--- |
| [`01_retries_and_callbacks.py`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/dags/05_monitoring_and_resilience/01_retries_and_callbacks.py) | Cấu hình thử lại hàm mũ cho tác vụ mạng rủi ro kết hợp các hàm xử lý Callback ghi log chi tiết lỗi khi có sự cố. |
| [`02_task_groups_ui.py`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/dags/05_monitoring_and_resilience/02_task_groups_ui.py) | Gom nhóm các tác vụ thành `extract_sources_group` và `transform_data_group` bằng Context Manager `with TaskGroup(...)`, tạo giao diện gọn gàng, có thể mở rộng/thu gọn trên Web UI. |

---

## 🔄 Chiến Lược Thử Lại (Retry Strategy)

```
Lần chạy 1 (Thất bại) ──[Chờ 10s]──▶ Lần chạy 2 (Thất bại) ──[Chờ 20s - Backoff]──▶ Lần chạy 3 (Thất bại) ──[Chờ 40s]──▶ on_failure_callback (Báo động Đỏ ❌)
```

- **Exponential Backoff**: Tăng dần thời gian chờ giữa các lần thử lại nhằm giảm tải áp lực tức thời lên server đích (Third-party API / Database).

---

## 🚀 Hướng Dẫn Kiểm Thử & Thực Thi

### 1. Kiểm tra cú pháp DAG
```bash
python dags/05_monitoring_and_resilience/01_retries_and_callbacks.py
python dags/05_monitoring_and_resilience/02_task_groups_ui.py
```

### 2. Kiểm thử Callback giả lập
```bash
airflow tasks test 01_retries_and_callbacks resilient_api_call 2024-01-01
```
