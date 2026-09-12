# Airflow CLI Cheatsheet & Top Phỏng Vấn

Tập hợp các câu lệnh dòng lệnh (CLI) thường dùng nhất khi vận hành Airflow và các câu hỏi phỏng vấn phổ biến.

---

## 1. Các Câu Lệnh CLI Thường Dùng

### 1.1. Kiểm Tra và Test DAGs
```bash
# 1. Liệt kê danh sách tất cả DAGs
airflow dags list

# 2. Kiểm tra lỗi cú pháp và import DAGs
airflow dags list-import-errors

# 3. Test nhanh 1 task mà không lưu trạng thái vào database hay kích hoạt downstream
airflow tasks test <dag_id> <task_id> <YYYY-MM-DD>
# Ví dụ:
airflow tasks test 01_first_dag_classic task_python_logic 2024-01-01

# 4. Liệt kê các task trong một DAG
airflow tasks list <dag_id> --tree
```

### 1.2. Điều Khiển và Chạy Lại (Backfill)
```bash
# 1. Kích hoạt (Trigger) DAG chạy ngay lập tức
airflow dags trigger <dag_id>

# 2. Bật (Unpause) hoặc Tắt (Pause) DAG
airflow dags unpause <dag_id>
airflow dags pause <dag_id>

# 3. Chạy bù dữ liệu lịch sử (Backfill) cho một khoảng thời gian
airflow dags backfill <dag_id> --start-date 2024-01-01 --end-date 2024-01-07
```

### 1.3. Quản Lý Hệ Thống & Database
```bash
# 1. Khởi tạo / Nâng cấp metadata database
airflow db migrate

# 2. Tạo tài khoản quản trị viên Admin Web UI
airflow users create \
    --username admin \
    --password admin \
    --firstname Minh \
    --lastname Doan \
    --role Admin \
    --email admin@example.com
```

---

## 2. Các Câu Hỏi Phỏng Vấn Airflow Thường Gặp

### Q1: Execution Date (Logical Date) trong Airflow có ý nghĩa gì?
> **Trả lời**: `execution_date` (từ Airflow 2.2 đổi tên thành `logical_date`) đại diện cho **thời điểm bắt đầu của khoảng thời gian dữ liệu** (Data Interval Start) mà DAG chịu trách nhiệm xử lý, KHÔNG PHẢI thời gian thực tế mà DAG được kích hoạt (Run Date).

### Q2: Vì sao không nên viết logic nặng (DB connection, API call) ở Top-level code của file DAG?
> **Trả lời**: Scheduler định kỳ phân tích (parse) toàn bộ các file `.py` trong thư mục `dags/` mỗi vài giây một lần. Nếu có top-level code nặng, Scheduler sẽ bị nghẽn CPU, làm chậm toàn bộ hệ sinh thái lập lịch. Hãy luôn đặt logic vào bên trong hàm thực thi của Operator / `@task`.

### Q3: Khi nào nên dùng XCom và khi nào KHÔNG nên dùng XCom?
> **Trả lời**: 
> - **NÊN DÙNG**: Truyền các metadata nhỏ như ID, status, URL file kết quả, số lượng bản ghi đã xử lý (< 48KB).
> - **KHÔNG NÊN DÙNG**: Truyền DataFrame lớn, mảng dữ liệu khổng lồ. Với dữ liệu lớn, hãy lưu vào Data Lake (S3/GCS) hoặc Database và chỉ truyền đường dẫn file qua XCom.
