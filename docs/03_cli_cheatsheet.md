# 💡 Airflow CLI Cheatsheet & Top Câu Hỏi Phỏng Vấn Data Engineer

Tổng hợp các lệnh dòng lệnh (Airflow CLI) thông dụng nhất cho Data Engineer / DevOps khi quản trị cụm Airflow cùng bộ câu hỏi phỏng vấn thực tế.

---

## 1. Cẩm Nang Lệnh Airflow CLI Thực Chiến

### 1.1. Quản Lý & Kiểm Thử DAGs (DAG Management)
```bash
# Liệt kê tất cả các DAGs đang có trong hệ thống
airflow dags list

# Kiểm tra lỗi import và cú pháp Python trong toàn bộ thư mục dags/
airflow dags list-import-errors

# Hiển thị cấu trúc cây phụ thuộc của các task trong một DAG
airflow tasks list <dag_id> --tree

# Kích hoạt chạy một DAG ngay lập tức với cấu hình JSON tùy biến
airflow dags trigger <dag_id> --conf '{"date": "2024-01-01", "mode": "full"}'

# Bật hoặc tắt trạng thái lập lịch của một DAG
airflow dags unpause <dag_id>
airflow dags pause <dag_id>

# Xóa lịch sử chạy của một DAG
airflow dags delete <dag_id>
```

### 1.2. Kiểm Thử Task Đơn Lẻ (Task Testing - Không tác động DB)
```bash
# Test chạy một task đơn lẻ mà không cần bật DAG, không phụ thuộc upstream hay ghi DB
airflow tasks test <dag_id> <task_id> <YYYY-MM-DD>
# Ví dụ:
airflow tasks test 01_first_dag_classic task_python_logic 2024-01-01

# Hiển thị log của một task instance cụ thể
airflow tasks logs <dag_id> <task_id> <execution_date>
```

### 1.3. Lệnh Chạy Bù Dữ Liệu Lịch Sử (Backfill)
```bash
# Kiểm tra trước danh sách DagRun sẽ được kích hoạt mà không thực thi thật
airflow dags backfill <dag_id> \
    --start-date 2024-01-01 \
    --end-date 2024-01-07 \
    --dry-run

# Chạy thực tế và reset trạng thái nếu trước đó đã từng chạy
airflow dags backfill <dag_id> \
    --start-date 2024-01-01 \
    --end-date 2024-01-07 \
    --reset-dagruns
```

### 1.4. Quản Trị Variables & Connections
```bash
# Xuất toàn bộ Variables ra file JSON (Dùng backup/migration)
airflow variables export variables_backup.json

# Nhập Variables từ file JSON vào Airflow
airflow variables import variables_backup.json

# Quản lý Connection qua CLI
airflow connections list
airflow connections export connections_backup.yaml
airflow connections import connections_backup.yaml
```

### 1.5. Bảo Trì Hệ Thống & Database (Maintenance)
```bash
# Nâng cấp migration cơ sở dữ liệu metadata
airflow db migrate

# Dọn dẹp dữ liệu log và task instances cũ để giảm tải metadata DB
airflow db clean --clean-before-timestamp '2024-01-01' --yes

# Tạo tài khoản quản trị viên Admin Web UI
airflow users create \
    --username admin \
    --password admin \
    --firstname Minh \
    --lastname Doan \
    --role Admin \
    --email admin@example.com
```

---

## 2. Top Câu Hỏi Phỏng Vấn Airflow (Senior Data Engineer)

### Q1: Execution Date (Logical Date) trong Airflow có ý nghĩa gì?
> **Trả lời**: `execution_date` (từ Airflow 2.2 đổi tên thành `logical_date`) đại diện cho **thời điểm bắt đầu của khoảng thời gian dữ liệu** (Data Interval Start) mà DAG chịu trách nhiệm xử lý, KHÔNG PHẢI thời gian thực tế mà DAG được kích hoạt (Run Date).

### Q2: Vì sao không nên viết logic nặng (DB connection, API call) ở Top-level code của file DAG?
> **Trả lời**: Scheduler định kỳ phân tích (parse) toàn bộ các file `.py` trong thư mục `dags/` mỗi vài giây một lần. Nếu có top-level code nặng, Scheduler sẽ bị nghẽn CPU, làm chậm toàn bộ hệ sinh thái lập lịch. Hãy luôn đặt logic vào bên trong hàm thực thi của Operator hoặc `@task`.

### Q3: Khi nào nên dùng XCom và khi nào KHÔNG nên dùng XCom?
> **Trả lời**:
> - **NÊN DÙNG**: Truyền các metadata nhỏ như ID, status, URL file kết quả, số lượng bản ghi đã xử lý (< 48KB).
> - **KHÔNG NÊN DÙNG**: Truyền DataFrame lớn, mảng dữ liệu khổng lồ. Với dữ liệu lớn, hãy lưu vào Data Lake (S3/GCS) hoặc Database và chỉ truyền đường dẫn file qua XCom.

### Q4: Sự khác nhau giữa `mode="poke"` và `mode="reschedule"` của Sensor là gì?
> **Trả lời**: `mode="poke"` sẽ chiếm dụng một Worker Slot liên tục từ lúc bắt đầu cho tới khi điều kiện thỏa mãn hoặc timeout, gây nghẽn pool worker. `mode="reschedule"` sẽ giải phóng slot worker ngay sau mỗi lần kiểm tra không thành công và chỉ xin cấp lại slot khi đến kỳ `poke_interval` tiếp theo.

### Q5: Tại sao SubDAG bị deprecated trong Airflow 2.x và được thay thế bằng gì?
> **Trả lời**: SubDAG tạo ra một DAG độc lập với scheduler riêng, dễ gây ra tình trạng khóa chết (Deadlock) cạnh tranh worker slots của cụm. SubDAG đã được thay thế hoàn toàn bằng **TaskGroup**, vốn chỉ là cơ chế tổ chức giao diện trên UI mà không tiêu tốn thêm tài nguyên thực thi.

### Q6: Làm thế nào để đảm bảo tính Lũy đạo (Idempotency) khi viết DAG?
> **Trả lời**: Bằng cách sử dụng các thao tác nạp dữ liệu có tính chất ghi đè hoặc UPSERT (`INSERT OR REPLACE`, `MERGE INTO`, hoặc xóa phân vùng cũ `DELETE WHERE partition = ...` trước khi nạp mới), đảm bảo chạy lại DAG nhiều lần với cùng một logical_date đều cho ra kết quả duy nhất không bị trùng lặp.
