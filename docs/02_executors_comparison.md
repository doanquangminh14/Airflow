# So Sánh Các Loại Executor Trong Airflow

Executor định nghĩa cách Airflow phân phối và thực thi các tasks. Dưới đây là bảng so sánh chi tiết giữa 4 Executor phổ biến nhất:

---

## 1. Bảng So Sánh Tổng Quan

| Tiêu Chí | SequentialExecutor | LocalExecutor | CeleryExecutor | KubernetesExecutor |
| :--- | :--- | :--- | :--- | :--- |
| **Độ Phức Tạp Cài Đặt** | Rất thấp (Mặc định) | Thấp | Trung bình - Cao | Cao |
| **Chạy Song Song (Parallelism)** | ❌ Không (Tuần tự 1 task) | ✅ Có (Đa tiến trình trên 1 máy) | ✅ Rất mạnh (Phân tán qua nhiều máy) | ✅ Cực mạnh (Dynamic auto-scaling) |
| **Database Hỗ Trợ** | SQLite | PostgreSQL / MySQL | PostgreSQL / MySQL | PostgreSQL / MySQL |
| **Hạ Tầng Cần Thêm** | Không có | Không có | Redis / RabbitMQ Message Broker | Cụm Kubernetes Cluster |
| **Môi Trường Phù Hợp** | Học tập, thử nghiệm cục bộ | Doanh nghiệp nhỏ & vừa, server đơn | Doanh nghiệp lớn, tải công việc cao | Cloud-native, phân lập môi trường tuyệt đối |

---

## 2. Chi Tiết Từng Loại Executor

### 2.1. SequentialExecutor
- **Cơ chế**: Chạy 1 task tại một thời điểm trên cùng 1 process với Scheduler.
- **Ưu điểm**: Không cần cài đặt database ngoài, mở lên là chạy ngay.
- **Nhược điểm**: Không thể scale, không hỗ trợ production.

### 2.2. LocalExecutor
- **Cơ chế**: Sinh ra các subprocess để chạy nhiều task đồng thời trên cùng một máy chủ chứa Scheduler.
- **Ưu điểm**: Cấu hình đơn giản, hiệu năng tốt, không cần Message Broker.
- **Nhược điểm**: Bị giới hạn bởi CPU/RAM của duy nhất một máy chủ vật lý.

### 2.3. CeleryExecutor
- **Cơ chế**: Sử dụng Celery và Message Broker (Redis/RabbitMQ) để phân phối task tới một cụm gồm nhiều Celery Worker chạy trên nhiều server khác nhau.
- **Ưu điểm**: Phân tán tải tốt, mở rộng theo chiều ngang (scale horizontal).
- **Nhược điểm**: Cần duy trì worker 24/7 (tốn tài nguyên nhàn rỗi) và cần quản lý nhiều thành phần phụ trợ.

### 2.4. KubernetesExecutor
- **Cơ chế**: Mỗi task instance khi được kích hoạt sẽ tạo mới 1 Pod riêng biệt trên Kubernetes. Khi chạy xong, Pod tự hủy.
- **Ưu điểm**:
  - Tự động scale từ 0 đến hàng ngàn task.
  - Mỗi task có thể có Docker Image, CPU/RAM limits và thư viện Python độc lập hoàn toàn.
  - Tiết kiệm chi phí vì chỉ tiêu tốn tài nguyên khi có task chạy.
- **Nhược điểm**: Có độ trễ khởi động Pod (startup latency từ vài giây đến nửa phút).
