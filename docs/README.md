# 📚 Airflow Documentation & Architecture Guides

Thư mục `docs/` chứa tài liệu lý thuyết chuyên sâu về kiến trúc hệ thống, so sánh các bộ điều phối (Executors), sổ tay tra cứu lệnh Airflow CLI thực chiến và bộ câu hỏi phỏng vấn Senior Data Engineer.

---

## 📑 Danh Sách Tài Liệu

| Tài Liệu | Nội Dung Chính & Điểm Nổi Bật |
| :--- | :--- |
| [`01_airflow_architecture.md`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/docs/01_airflow_architecture.md) | Kiến trúc tổng thể 6 thành phần: Webserver, Scheduler, Metadata Database, Executor, Worker, và Triggerer. Sơ đồ tương tác luồng thực thi và vòng đời Task Instance. |
| [`02_executors_comparison.md`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/docs/02_executors_comparison.md) | Phân tích và so sánh 5 loại Executor: `SequentialExecutor`, `LocalExecutor`, `CeleryExecutor`, `KubernetesExecutor`, và `CeleryKubernetesExecutor` (AWS MWAA & GCP Composer mapping). |
| [`03_cli_cheatsheet.md`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/docs/03_cli_cheatsheet.md) | Tổng hợp các lệnh CLI thông dụng nhất cho Developer & DevOps (quản lý DAGs, Tasks, Users, Variables/Connections Import/Export, Backfill & DB Maintenance) cùng bộ câu hỏi phỏng vấn chuẩn mực. |

---

## 💡 Lộ Trình Đọc Khuyến Nghị

1. **Khái niệm & Bản chất**: Đọc [`01_airflow_architecture.md`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/docs/01_airflow_architecture.md) để nắm toàn cảnh cách các daemon giao tiếp và cơ chế Deferrable Operators qua Triggerer.
2. **Quy mô & Hạ tầng**: Đọc [`02_executors_comparison.md`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/docs/02_executors_comparison.md) để lựa chọn kiến trúc scale và dự trù chi phí cho Data Platform.
3. **Thao tác & Vận hành**: Tham khảo [`03_cli_cheatsheet.md`](file:///c:/Users/Minh%20Doan/repo_github/Airflow/docs/03_cli_cheatsheet.md) khi debug hoặc thao tác trên server thực tế và chuẩn bị cho các vòng phỏng vấn kỹ thuật.
