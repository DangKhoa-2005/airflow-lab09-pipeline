import great_expectations as gx
from great_expectations.exceptions import DataContextError, ExpectationNotFoundError
# Import ValidationResult để kiểm tra kiểu kết quả
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult
import pandas as pd
import logging
from pathlib import Path
import sys
import datetime

# --- Cấu hình Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logging.getLogger("great_expectations").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- Hằng số và Đường dẫn ---
DATA_DIR = Path("data")
INPUT_CSV_FILE = DATA_DIR / "202306-divvy-tripdata.csv" # Đảm bảo tên file đúng
GX_DIR = Path("./great_expectations") # Thư mục cấu hình GX

# --- Hàm chính ---

def run_data_quality_check():
    """Thực hiện kiểm tra chất lượng dữ liệu bằng Great Expectations (V3 API)."""
    logger.info("--- Bắt đầu Exercise 10: Data Quality with Great Expectations (V3 API) ---")

    # --- 1. Khởi tạo Data Context ---
    context = None
    try:
        context = gx.get_context(project_root_dir=str(GX_DIR))
        logger.info(f"Sử dụng/Tạo Data Context tại: {GX_DIR.resolve()}")
    except Exception as e:
         logger.error(f"Lỗi không xác định khi khởi tạo Data Context: {e}", exc_info=True)
         sys.exit(1)

    # --- 2. Đọc dữ liệu nguồn bằng Pandas ---
    logger.info(f"Đang đọc file CSV: {INPUT_CSV_FILE}")
    try:
        df = pd.read_csv(INPUT_CSV_FILE)
        df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
        df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')
        df.dropna(subset=['started_at', 'ended_at'], inplace=True)
        logger.info(f"Đọc và xử lý ngày tháng thành công. Shape: {df.shape}")
        df['start_date'] = df['started_at'].dt.date
        df['end_date'] = df['ended_at'].dt.date
    except FileNotFoundError:
        logger.error(f"Không tìm thấy file CSV: {INPUT_CSV_FILE}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Lỗi khi đọc hoặc xử lý file CSV: {e}", exc_info=True)
        sys.exit(1)

    # --- 3. Thêm Datasource và Data Asset (Fluent API) ---
    # Vẫn cần bước này để get_validator hoạt động đúng cách với batch request
    datasource_name = "my_pandas_datasource_v3"
    data_asset_name = "divvy_trips_v3"
    batch_request = None
    try:
        datasource = context.sources.add_or_update_pandas(name=datasource_name)
        logger.info(f"Đã thêm/cập nhật Datasource: {datasource_name}")
        data_asset = datasource.add_dataframe_asset(name=data_asset_name, dataframe=df)
        logger.info(f"Đã thêm/cập nhật Data Asset '{data_asset_name}'.")
        batch_request = data_asset.build_batch_request()
        logger.info("Đã tạo Batch Request từ Data Asset.")
    except Exception as e:
        logger.error(f"Lỗi khi cấu hình Datasource/Data Asset: {e}", exc_info=True)
        sys.exit(1)

    # --- 4. Tạo hoặc lấy Expectation Suite ---
    expectation_suite_name = "divvy_trip_duration_suite_v3"
    try:
        context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
        logger.info(f"Đã tạo/cập nhật Expectation Suite: {expectation_suite_name}")
        suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
        if suite is None:
             raise ValueError(f"Không thể lấy được Expectation Suite: {expectation_suite_name}")
    except Exception as e:
        logger.error(f"Lỗi khi tạo/lấy Expectation Suite: {e}", exc_info=True)
        sys.exit(1)

    # --- 5. Thêm Expectation vào Suite ---
    logger.info("Thêm Expectation vào Suite: expect_column_pair_values_to_be_equal")
    validator = None # Khởi tạo validator
    try:
        # Lấy validator liên kết với dữ liệu và suite
        validator = context.get_validator(
            batch_request=batch_request, # Sử dụng batch request đã tạo
            expectation_suite=suite
        )
        logger.info(f"Đã lấy Validator cho suite: {validator.expectation_suite_name}")

        # Tạo configuration cho expectation
        expectation_config = gx.core.ExpectationConfiguration(
            expectation_type="expect_column_pair_values_to_be_equal",
            kwargs={
                "column_A": "start_date",
                "column_B": "end_date",
                "mostly": 0.90
            }
        )
        # Bỏ qua bước xóa expectation cũ
        suite.add_expectation(expectation_configuration=expectation_config)
        context.save_expectation_suite(expectation_suite=suite)
        logger.info("Đã thêm/cập nhật và lưu Expectation vào Suite.")

    except Exception as e:
         logger.error(f"Lỗi khi làm việc với Validator hoặc Expectation Suite: {e}", exc_info=True)
         sys.exit(1)


    # --- 6. Chạy Validation trực tiếp bằng Validator ---
    if validator is None:
         logger.error("Không thể tạo validator để chạy validation.")
         sys.exit(1)

    logger.info(f"Đang chạy validation với Validator...")
    validation_result: Optional[ExpectationSuiteValidationResult] = None
    try:
        # <<< THAY ĐỔI: Sử dụng validator.validate() >>>
        validation_result = validator.validate()
        logger.info("Chạy validation bằng validator thành công.")

    except Exception as e:
        logger.error(f"Lỗi khi chạy validator.validate(): {e}", exc_info=True)
        sys.exit(1)

    # --- 7. Xử lý và In kết quả ---
    logger.info("\n--- Kết quả Kiểm tra Chất lượng Dữ liệu ---")
    success = False # Mặc định là thất bại
    if validation_result:
        success = validation_result.success # Lấy trạng thái từ ValidationResult
        print(f"Validation Success: {success}")

        if success:
            logger.info(">>> Validation THÀNH CÔNG! Tất cả Expectations đã đạt.")
        else:
            logger.error(">>> Validation THẤT BẠI! Có Expectation không đạt.")
            try:
                print("\nChi tiết Validation Result:")
                # In kết quả ValidationResult (thường gọn hơn CheckpointResult)
                print(validation_result.to_json_dict())
            except Exception as e_print:
                logger.error(f"Lỗi khi cố gắng in chi tiết kết quả validation: {e_print}")
    else:
         logger.error("Không có kết quả validation để hiển thị.")


    logger.info("--- Kết thúc Exercise 10 ---")

    # Thoát với mã lỗi nếu validation thất bại
    if not success:
        sys.exit(1)

# --- Chạy hàm chính ---
if __name__ == "__main__":
    run_data_quality_check()
