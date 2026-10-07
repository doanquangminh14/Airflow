# ⚙️ So Sánh Chuyên Sâu Các Loại Executor Trong Airflow

Executor định nghĩa cơ chế mà qua đó Apache Airflow phân phối và thực thi các tasks. Việc lựa chọn đúng Executor quyết định khả năng mở rộng (Scalability), độ ổn định và chi phí vận hành hạ tầng của hệ thống Data Platform.

---

## 1. Bảng Ma Trận So Sánh Các Executor Phổ Biến

| Tiêu Chí Đánh Giá | SequentialExecutor | LocalExecutor | CeleryExecutor | KubernetesExecutor | CeleryKubernetesExecutor (Hybrid) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Mức độ phức tạp triển khai** | Rất thấp (Mặc định) | Thấp | Trung bình - Cao | Cao | Rất cao |
| **Khả năng chạy song song** | ❌ 1 task tại 1 thời điểm | ✅ Đa tiến trình trên 1 máy | ✅ Phân tán đa máy chủ | ✅ Cực mạnh (Dynamic auto-scale) | ✅ Tối ưu hóa hỗn hợp |
| **Yêu cầu Database** | SQLite | PostgreSQL / MySQL | PostgreSQL / MySQL | PostgreSQL / MySQL | PostgreSQL / MySQL |
| **Hạ tầng bổ sung cần thiết** | Không | Không | Redis / RabbitMQ Broker | Cụm Kubernetes Cluster | K8s Cluster + Redis Broker |
| **Độ trễ khởi động Task (Latency)** | Rất thấp (< 0.1s) | Rất thấp (< 0.5s) | Thấp (1s - 2s) | Trung bình (5s - 30s tạo Pod) | Thấp cho task thường, K8s cho task nặng |
| **Cô lập môi trường (Isolation)** | Không | Không | Không | ✅ Tuyệt đối (Container Pod riêng) | ✅ Linh hoạt cấu hình theo task |
| **Use Case điển hình** | Học tập, thử nghiệm local | Doanh nghiệp SME, server đơn | Workload đồng nhất, tải lớn | Cloud-native, môi trường đa ngôn ngữ | Doanh nghiệp lớn tối ưu chi phí |

---

## 2. Chi Tiết Kiến Trúc Từng Loại Executor

### 2.1. SequentialExecutor
- **Cơ chế**: Chạy từng task một cách tuần tự trên cùng một tiến trình với Scheduler, sử dụng backend SQLite.
- **Ưu điểm**: Không cần bất kỳ cài đặt phụ trợ nào, khởi động tức thì.
- **Nhược điểm**: Không hỗ trợ chạy song song, không bao giờ được dùng cho Production.

### 2.2. LocalExecutor
- **Cơ chế**: Sinh ra các tiến trình con (`multiprocessing.Process`) trên cùng một máy chủ để chạy nhiều task đồng thời.
- **Ưu điểm**: Cấu hình đơn giản, không cần Redis hay Message Queue, độ trễ cực thấp.
- **Nhược điểm**: Bị giới hạn cứng bởi số lõi CPU và dung lượng RAM của một máy chủ vật lý.

### 2.3. CeleryExecutor
- **Cơ chế**: Sử dụng Celery phân phối nhiệm vụ qua Message Broker (Redis/RabbitMQ). Các Celery Worker Nodes liên tục lấy task từ queue về chạy.
- **Ưu điểm**: Khả năng mở rộng theo chiều ngang (Horizontal Scaling) tốt, xử lý hàng chục ngàn task/ngày với độ trễ thấp.
- **Nhược điểm**: Phải duy trì worker chạy 24/7 gây lãng phí tài nguyên ngoài giờ cao điểm; các worker dùng chung môi trường Python nên dễ xung đột thư viện dependencies.

### 2.4. KubernetesExecutor
- **Cơ chế**: Mỗi TaskInstance khi được kích hoạt sẽ tạo một Kubernetes Pod độc lập. Khi task chạy xong, Pod tự động bị hủy và giải phóng tài nguyên.
- **Ưu điểm**:
  - Tự động scale từ 0 lên hàng ngàn Pods theo nhu cầu thực tế (Scale to Zero).
  - Cô lập tài nguyên và môi trường hoàn hảo: Mỗi task có thể sử dụng Docker Image riêng biệt, cấu hình CPU/RAM requests/limits riêng.
- **Nhược điểm**: Tốn vài giây đến vài chục giây để kéo image và khởi động Pod (Startup overhead).

### 2.5. CeleryKubernetesExecutor (Kiến Trúc Hỗn Hợp)
- Tận dụng thế mạnh của cả hai: Các task nhanh/nhẹ (thời lượng ngắn) chạy trên Celery Workers để có độ trễ bằng không; các task nặng (Spark submit, huấn luyện ML, tốn nhiều RAM) được điều hướng sang chạy trên Kubernetes Pods riêng.
- Được sử dụng phổ biến trong các kiến trúc dịch vụ được quản lý như **Amazon MWAA (Managed Workflows for Apache Airflow)** và **Google Cloud Composer**.
