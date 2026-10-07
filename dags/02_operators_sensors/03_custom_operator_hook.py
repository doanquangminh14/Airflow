"""
==============================================================================
MODULE 02: TẠO VÀ SỬ DỤNG CUSTOM OPERATOR & HOOK
==============================================================================
Mục tiêu bài học:
1. Cách tự định nghĩa một Custom Operator kế thừa BaseOperator.
2. Thiết lập template_fields để hỗ trợ biểu thức Jinja ({{ ds }}).
3. Đặt màu sắc nhận diện trực quan trên Web UI bằng ui_color và ui_fgcolor.
4. Xử lý validation, ném lỗi kiểm soát và trả kết quả metadata vào XCom.
==============================================================================
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Sequence

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")


class CleanAndValidateCsvOperator(BaseOperator):
    """Custom Operator kiểm tra định dạng và chất lượng tệp dữ liệu CSV đầu vào.

    Attributes:
        template_fields: Danh sách các trường được Jinja engine diễn giải.
        ui_color: Màu nền của task box trên Airflow Web UI.
        ui_fgcolor: Màu chữ của task box trên Airflow Web UI.
    """

    template_fields: Sequence[str] = ("source_name", "partition_date")
    ui_color: str = "#e1f5fe"
    ui_fgcolor: str = "#01579b"

    def __init__(
        self,
        source_name: str,
        partition_date: str = "{{ ds }}",
        min_rows: int = 1,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.source_name = source_name
        self.partition_date = partition_date
        self.min_rows = min_rows

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Phương thức thực thi chính của Custom Operator.

        Args:
            context: Ngữ cảnh thực thi của TaskInstance.

        Returns:
            Dict[str, Any]: Metadata chất lượng dữ liệu được lưu vào XCom.
        """
        self.log.info("=" * 60)
        self.log.info("Bắt đầu kiểm tra dữ liệu từ nguồn: %s", self.source_name)
        self.log.info("Partition Date được render: %s", self.partition_date)

        # Giả lập đọc và kiểm định 150 bản ghi
        simulated_row_count = 150
        if simulated_row_count < self.min_rows:
            error_msg = (
                f"LỖI CHẤT LƯỢNG DỮ LIỆU: Số lượng dòng ({simulated_row_count}) "
                f"nhỏ hơn ngưỡng tối thiểu yêu cầu ({self.min_rows})!"
            )
            self.log.error(error_msg)
            raise ValueError(error_msg)

        self.log.info("Kiểm định thành công! Tổng số dòng hợp lệ: %s >= %s", simulated_row_count, self.min_rows)
        self.log.info("=" * 60)

        # Giá trị trả về sẽ tự động được lưu vào XCom với key='return_value'
        return {
            "source_name": self.source_name,
            "partition_date": self.partition_date,
            "valid_rows": simulated_row_count,
            "status": "PASSED",
        }


DAG_DOC_MD = """
### 🛠️ DAG: Custom Operator & Hook Architecture
DAG minh họa việc mở rộng Airflow thông qua Custom Operator tự định nghĩa:
- **CleanAndValidateCsvOperator**: Kế thừa `BaseOperator`, hỗ trợ `template_fields`, tùy biến màu sắc UI (`ui_color`).
- **PythonOperator downstream**: Nhận metadata kết quả thẩm định trực tiếp qua `xcom_pull`.
"""

default_args: Dict[str, Any] = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="03_custom_operator_hook",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["module_02", "custom_operator", "plugins"],
    doc_md=DAG_DOC_MD,
) as dag:

    # 1. Khởi tạo Custom Operator với Jinja template
    validate_sales_data = CleanAndValidateCsvOperator(
        task_id="validate_sales_data",
        source_name="sales_report_{{ ds }}.csv",
        partition_date="{{ ds }}",
        min_rows=10,
        doc_md="Thực thi Custom Operator kiểm tra chất lượng tệp CSV bán hàng.",
    )

    # 2. Task downstream đọc kết quả kiểm định qua XCom
    def summarize_results(**context: Any) -> None:
        ti = context["ti"]
        validation_result = ti.xcom_pull(task_ids="validate_sales_data")
        logger.info("Nhận kết quả từ Custom Operator qua XCom: %s", validation_result)
        logger.info(
            "Tệp: %s | Trạng thái: %s | Số dòng: %s",
            validation_result.get("source_name"),
            validation_result.get("status"),
            validation_result.get("valid_rows"),
        )

    report_summary = PythonOperator(
        task_id="report_summary",
        python_callable=summarize_results,
        provide_context=True,
        doc_md="Tổng hợp và ghi nhận báo cáo chất lượng dữ liệu.",
    )

    # Thiết lập chuỗi quan hệ
    validate_sales_data >> report_summary
