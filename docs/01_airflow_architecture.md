# Kiến Trúc Tổng Quan Của Apache Airflow

Apache Airflow là một nền tảng mã nguồn mở hàng đầu dùng để lập lịch, điều phối và giám sát các luồng công việc (Workflows/Pipelines).

---

## 1. Các Thành Phần Cốt Lõi (Core Components)

```
                       +-------------------+
                       |    Webserver      |  <-- UI tương tác người dùng
                       +---------+---------+
                                 |
                                 v
+------------------+   +---------+---------+   +-------------------+
|  DAGs Directory  |-->|     Scheduler     |-->|   Workers / Pool  |
+------------------+   +---------+---------+   +-------------------+
                                 |                       |
                                 +----------+------------+
                                            |
                                            v
                                 +-------------------+
                                 |    Metadata DB    |  (PostgreSQL/MySQL)
                                 +-------------------+
```

### 1.1. Airflow Webserver
- Cung cấp giao diện web trực quan (UI) tại cổng mặc định `8080`.
- Cho phép xem trạng thái DAGs, lịch sử thực thi, logs từng task, trigger DAG thủ công, quản lý Variables và Connections.

### 1.2. Airflow Scheduler
- "Trái tim" của Airflow.
- Định kỳ quét thư mục `dags/`, phân tích code Python, kiểm tra lịch trình, và gửi các task đã thỏa mãn điều kiện tới hàng đợi (Executor).

### 1.3. Metadata Database
- Lưu trữ toàn bộ trạng thái của DAGs, Task Instances, XComs, Connections, Variables, Người dùng và Lịch sử chạy.
- Thường sử dụng **PostgreSQL** hoặc **MySQL** trong môi trường Production (tránh dùng SQLite cho Production vì không hỗ trợ ghi đồng thời).

### 1.4. Executor & Workers
- **Executor**: Cơ chế quyết định *cách thức* và *nơi* task sẽ được thực thi (ví dụ: Local, Celery, Kubernetes).
- **Workers**: Các tiến trình hoặc Pods trực tiếp chạy mã nguồn task.

### 1.5. Triggerer (Airflow 2.2+)
- Chạy tiến trình nền không đồng bộ (`asyncio`) hỗ trợ **Deferrable Operators**.
- Giúp giải phóng hoàn toàn slot worker khi đang chờ sự kiện bên ngoài (tiết kiệm 80-90% chi phí tài nguyên so với Sensor thông thường).

---

## 2. Vòng Đời Của Một Task (Task Lifecycle)

Một task trong Airflow trải qua các trạng thái:
1. `none`: Chưa được lập lịch.
2. `scheduled`: Scheduler đã xác định task đủ điều kiện để chạy.
3. `queued`: Task đã được gửi vào hàng đợi của Executor.
4. `running`: Worker đang thực thi code của task.
5. `success` / `failed`: Task hoàn thành thành công hoặc gặp lỗi.
6. `up_for_retry`: Task lỗi nhưng còn số lượt retry, đang chờ thử lại.
7. `skipped`: Bị bỏ qua do điều kiện phân nhánh (Branching).
