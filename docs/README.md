# 📚 Airflow Documentation & Architecture Guides

Thư mục `docs/` chứa tài liệu lý thuyết chuyên sâu về kiến trúc hệ thống, so sánh các bộ điều phối (Executors) và sổ tay tra cứu lệnh Airflow CLI.

---

## 📑 Danh Sách Tài Liệu

| Tài Liệu | Nội Dung Chính |
| :--- | :--- |
| [`01_airflow_architecture.md`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/docs/01_airflow_architecture.md) | Kiến trúc tổng thể 6 thành phần: Webserver, Scheduler, Metadata Database, Executor, Worker, và Triggerer. Sơ đồ tương tác luồng thực thi. |
| [`02_executors_comparison.md`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/docs/02_executors_comparison.md) | Phân tích và so sánh các loại Executor: `SequentialExecutor`, `LocalExecutor`, `CeleryExecutor`, `KubernetesExecutor` (Ưu/nhược điểm & use cases thực tế). |
| [`03_cli_cheatsheet.md`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/docs/03_cli_cheatsheet.md) | Tổng hợp các lệnh CLI thông dụng nhất cho Developer & DevOps (quản lý DAGs, Tasks, Users, Variables, Connections, Backfill & DB Init). |

---

## 💡 Lộ Trình Đọc Khuyến Nghị

1. Đọc [`01_airflow_architecture.md`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/docs/01_airflow_architecture.md) để nắm toàn cảnh cách các daemon giao tiếp.
2. Đọc [`02_executors_comparison.md`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/docs/02_executors_comparison.md) để lựa chọn kiến trúc scale phù hợp cho dự án của bạn.
3. Tham khảo [`03_cli_cheatsheet.md`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/docs/03_cli_cheatsheet.md) khi debug hoặc thao tác trên server thực tế.
