import logging
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, LongType, DateType, IntegerType
from typing import Optional, List, Tuple
import re
import zipfile # Import zipfile
import shutil # Import shutil để xóa thư mục tạm

# --- Cấu hình Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- Hằng số và Đường dẫn ---
DATA_DIR = Path("data")
INPUT_FILE_ZIP = DATA_DIR / "hard-drive-2022-01-01-failures.csv.zip"
# Tạo thư mục tạm để giải nén (bên trong container)
EXTRACT_DIR = Path("/tmp/extracted_csv")
# Đường dẫn file CSV sau khi giải nén (sẽ được xác định sau)
EXTRACTED_CSV_PATH: Optional[Path] = None

# --- Khởi tạo SparkSession ---
def create_spark_session(app_name="Exercise7"):
    """Khởi tạo và trả về một SparkSession."""
    logger.info(f"Đang khởi tạo SparkSession: {app_name}")
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        logger.info("SparkSession đã được tạo thành công.")
        return spark
    except Exception as e:
        logger.error(f"Lỗi khi tạo SparkSession: {e}", exc_info=True)
        raise

# --- Các Hàm Xử lý ---

def unzip_csv_file(zip_path: Path, extract_dir: Path) -> Optional[Path]:
    """Giải nén file CSV từ file zip vào thư mục tạm."""
    if not zip_path.is_file():
        logger.error(f"Không tìm thấy file zip: {zip_path}")
        return None

    logger.info(f"Đang giải nén file từ {zip_path} vào {extract_dir}...")
    try:
        extract_dir.mkdir(parents=True, exist_ok=True) # Tạo thư mục giải nén
        extracted_csv = None
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            csv_files_in_zip = [name for name in zip_ref.namelist() if name.lower().endswith('.csv')]
            if not csv_files_in_zip:
                logger.error(f"Không tìm thấy file CSV nào trong {zip_path}")
                return None
            if len(csv_files_in_zip) > 1:
                logger.warning(f"Tìm thấy nhiều file CSV trong zip: {csv_files_in_zip}. Chỉ giải nén file đầu tiên: {csv_files_in_zip[0]}")

            csv_to_extract = csv_files_in_zip[0]
            zip_ref.extract(csv_to_extract, path=extract_dir)
            extracted_csv = extract_dir / csv_to_extract
            logger.info(f"Giải nén thành công file: {extracted_csv}")
        return extracted_csv
    except zipfile.BadZipFile:
        logger.error(f"File không hợp lệ hoặc không phải file zip: {zip_path}")
        return None
    except Exception as e:
        logger.error(f"Lỗi khi giải nén file {zip_path}: {e}", exc_info=True)
        return None

def clean_col_name(col_name: str) -> str:
    """Làm sạch tên cột."""
    # (Giữ nguyên hàm này)
    if not isinstance(col_name, str):
        logger.warning(f"Tên cột không phải string: {col_name}. Sẽ trả về 'invalid_col_name'.")
        return "invalid_col_name"
    if re.fullmatch(r'[\w]+', col_name):
         return col_name.lower()
    cleaned = re.sub(r'[^\w]+', '_', col_name)
    cleaned = re.sub(r'_+', '_', cleaned).strip('_')
    return cleaned.lower()

def read_drive_data(spark: SparkSession, csv_path: Path) -> Optional[DataFrame]:
    """Đọc dữ liệu ổ cứng từ file CSV đã giải nén."""
    # Hàm này giờ nhận đường dẫn file CSV đã giải nén
    logger.info(f"Đang đọc dữ liệu từ file CSV đã giải nén: {csv_path}")
    if not csv_path or not csv_path.is_file():
         logger.error(f"File CSV không hợp lệ hoặc không tồn tại: {csv_path}")
         return None
    try:
        df = spark.read.csv(
            str(csv_path), # Spark cần đường dẫn dạng string
            header=True,
            inferSchema=False,
            escape="\""
        )
        logger.info(f"Đọc dữ liệu thô thành công. Số lượng bản ghi ban đầu: {df.count()}")
        original_columns = df.columns
        logger.info(f"Tên cột gốc đọc từ header: {original_columns}")

        cleaned_columns = [clean_col_name(col) for col in original_columns]
        logger.info(f"Tên cột sau khi làm sạch (dự kiến): {cleaned_columns}")

        if len(cleaned_columns) != len(set(cleaned_columns)):
            logger.warning("Phát hiện tên cột bị trùng sau khi làm sạch!")

        rename_mapping = dict(zip(original_columns, cleaned_columns))
        select_expr = [F.col(old_col).alias(new_col) for old_col, new_col in rename_mapping.items()]
        df_renamed = df.select(*select_expr)
        final_columns = df_renamed.columns
        logger.info(f"Tên cột sau khi đổi tên trong DataFrame: {final_columns}")

        # --- Cast kiểu dữ liệu ---
        logger.info("Thực hiện cast kiểu dữ liệu...")
        df_casted = df_renamed

        date_col = "date"
        if date_col in final_columns:
            df_casted = df_casted.withColumn(date_col, F.to_date(F.col(date_col), "yyyy-MM-dd"))
            logger.info(f"Đã cast cột '{date_col}' sang DateType.")
        else:
            logger.error(f"Không tìm thấy cột '{date_col}' sau khi đổi tên để cast sang DateType!")
            return None

        capacity_col = "capacity_bytes"
        if capacity_col in final_columns:
            df_casted = df_casted.withColumn(capacity_col, F.col(capacity_col).cast(LongType()))
            logger.info(f"Đã cast cột '{capacity_col}' sang LongType.")
        else:
            logger.warning(f"Không tìm thấy cột '{capacity_col}' để cast sang LongType!")

        failure_col = "failure"
        if failure_col in final_columns:
            df_casted = df_casted.withColumn(failure_col, F.col(failure_col).cast(IntegerType()))
            logger.info(f"Đã cast cột '{failure_col}' sang IntegerType.")
        else:
            logger.warning(f"Không tìm thấy cột '{failure_col}' để cast sang IntegerType!")

        for col_name in final_columns:
            if col_name.startswith("smart_") and col_name.endswith("_raw"):
                try:
                    df_casted = df_casted.withColumn(col_name, F.col(col_name).cast(LongType()))
                except Exception as cast_err:
                    logger.warning(f"Không thể cast cột SMART '{col_name}' sang LongType: {cast_err}")

        logger.info("Cast kiểu dữ liệu hoàn tất.")
        df_casted.printSchema()
        return df_casted
    except Exception as e:
        logger.error(f"Lỗi khi đọc hoặc xử lý dữ liệu từ {csv_path}: {e}", exc_info=True)
        return None

# --- Các hàm xử lý còn lại (Giữ nguyên) ---
def add_source_file_column(df: DataFrame) -> DataFrame:
    logger.info("Câu 1: Thêm cột source_file...")
    # Lấy tên file zip gốc thay vì file tạm
    source_zip_name = INPUT_FILE_ZIP.name
    df_with_source = df.withColumn("source_file", F.lit(source_zip_name))
    return df_with_source

def add_file_date_column(df: DataFrame) -> DataFrame:
    logger.info("Câu 2: Thêm cột file_date...")
    if "source_file" not in df.columns:
        logger.error("Thiếu cột 'source_file' để trích xuất ngày.")
        return df
    df_with_date = df.withColumn("file_date_str", F.regexp_extract(F.col("source_file"), r'(\d{4}-\d{2}-\d{2})', 1))
    df_with_date = df_with_date.withColumn("file_date", F.to_date(F.col("file_date_str"), "yyyy-MM-dd"))
    return df_with_date

def add_brand_column(df: DataFrame) -> DataFrame:
    logger.info("Câu 3: Thêm cột brand...")
    model_col = "model"
    if model_col not in df.columns:
        logger.error(f"Không tìm thấy cột '{model_col}' để tạo cột brand.")
        return df
    split_col = F.split(F.col(model_col), " ", 2)
    df_with_brand = df.withColumn("brand",
        F.when(F.col(model_col).isNull(), "unknown")
         .when(F.size(split_col) > 1, split_col.getItem(0))
         .otherwise("unknown")
    )
    return df_with_brand

def add_storage_ranking_column(df: DataFrame) -> DataFrame:
    logger.info("Câu 4: Thêm cột storage_ranking...")
    model_col = "model"
    capacity_col = "capacity_bytes"
    if model_col not in df.columns or capacity_col not in df.columns:
         logger.error(f"Thiếu cột '{model_col}' hoặc '{capacity_col}' để xếp hạng dung lượng.")
         return df.withColumn("storage_ranking", F.lit(None).cast("int"))

    model_capacity_df = df.filter(F.col(capacity_col).isNotNull()) \
                          .select(model_col, capacity_col) \
                          .distinct()
    if not model_capacity_df.head(1):
        logger.warning("Không có dữ liệu capacity hợp lệ để xếp hạng.")
        return df.withColumn("storage_ranking", F.lit(None).cast("int"))

    window_spec = Window.orderBy(F.col(capacity_col).desc())
    model_ranking_df = model_capacity_df.withColumn("storage_ranking", F.dense_rank().over(window_spec))

    df_with_ranking = df.join(
        model_ranking_df.select(model_col, "storage_ranking"),
        on=model_col,
        how="left"
    )
    return df_with_ranking

def add_primary_key_column(df: DataFrame, unique_cols_cleaned: List[str]) -> DataFrame:
    logger.info(f"Câu 5: Thêm cột primary_key từ các cột: {unique_cols_cleaned}")
    missing_cols = [col for col in unique_cols_cleaned if col not in df.columns]
    if missing_cols:
        logger.error(f"Các cột để tạo khóa chính không tồn tại: {missing_cols}")
        return df

    existing_unique_cols = [c for c in unique_cols_cleaned if c in df.columns]
    if not existing_unique_cols:
        logger.error("Không có cột nào hợp lệ để tạo khóa chính.")
        return df

    logger.info(f"Sử dụng các cột sau để tạo PK: {existing_unique_cols}")
    concat_cols = [F.coalesce(F.col(c).cast(StringType()), F.lit("NULL")) for c in existing_unique_cols]
    df_with_pk = df.withColumn("pk_concat", F.concat_ws("|", *concat_cols))
    df_with_pk = df_with_pk.withColumn("primary_key", F.sha2(F.col("pk_concat"), 256))
    df_with_pk = df_with_pk.drop("pk_concat")
    return df_with_pk

# --- Luồng Thực thi Chính ---
if __name__ == "__main__":
    logger.info("--- Bắt đầu Exercise 7: PySpark Functions ---")

    spark = None
    extracted_csv_path = None # Biến lưu đường dẫn file CSV đã giải nén
    try:
        # Bước 1: Giải nén file zip
        extracted_csv_path = unzip_csv_file(INPUT_FILE_ZIP, EXTRACT_DIR)
        if not extracted_csv_path:
             raise FileNotFoundError("Không thể giải nén file CSV từ zip.")

        # Bước 2: Khởi tạo Spark và đọc file CSV đã giải nén
        spark = create_spark_session()
        drive_data_df = read_drive_data(spark, extracted_csv_path)

        if drive_data_df:
            # Bước 3: Áp dụng các biến đổi
            df1 = add_source_file_column(drive_data_df)
            df2 = add_file_date_column(df1)
            df3 = add_brand_column(df2)
            df4 = add_storage_ranking_column(df3)

            unique_identifier_columns_cleaned = ["date", "serial_number", "model"]
            final_df = add_primary_key_column(df4, unique_identifier_columns_cleaned)

            # Bước 4: Hiển thị kết quả
            logger.info("--- Schema cuối cùng ---")
            final_df.printSchema()
            logger.info("--- 10 dòng dữ liệu cuối cùng ---")
            final_df.show(10, truncate=80)

        else:
            logger.error("Không thể đọc hoặc xử lý dữ liệu CSV đã giải nén. Dừng xử lý.")
            exit(1)

    except Exception as e:
         logger.error(f"Lỗi không mong muốn trong quá trình xử lý chính: {e}", exc_info=True)
         exit(1)
    finally:
        # Dừng SparkSession
        if spark:
            logger.info("Đang dừng SparkSession...")
            spark.stop()
            logger.info("SparkSession đã dừng.")
        # Dọn dẹp thư mục tạm sau khi chạy xong
        if EXTRACT_DIR.exists():
            try:
                logger.info(f"Đang xóa thư mục tạm: {EXTRACT_DIR}")
                shutil.rmtree(EXTRACT_DIR)
                logger.info("Xóa thư mục tạm thành công.")
            except OSError as e:
                logger.error(f"Không thể xóa thư mục tạm {EXTRACT_DIR}: {e}")


    logger.info("--- Kết thúc Exercise 7 ---")

