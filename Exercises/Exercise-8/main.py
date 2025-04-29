import duckdb
import logging
from pathlib import Path
import re
import pandas as pd # Dùng pandas để chuẩn bị dữ liệu nếu cần, hoặc xử lý kết quả
from typing import Optional

# --- Cấu hình Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- Hằng số và Đường dẫn ---
DATA_DIR = Path("data")
REPORTS_DIR = Path("reports")
INPUT_CSV_FILE = DATA_DIR / "Electric_Vehicle_Population_Data.csv"
# Sử dụng database DuckDB trong bộ nhớ (hoặc chỉ định file nếu muốn lưu trữ)
DUCKDB_PATH = ":memory:"
# DUCKDB_PATH = "ev_database.duckdb" # Ví dụ lưu ra file

TABLE_NAME = "electric_vehicles"

# --- Các Hàm Hỗ trợ ---

def clean_col_name(col_name: str) -> str:
    """Làm sạch tên cột cho SQL: bỏ ký tự đặc biệt, đổi thành snake_case."""
    if not isinstance(col_name, str):
        return "invalid_col_name"
    # Thay thế các ký tự không phải chữ/số bằng underscore
    cleaned = re.sub(r'\W+', '_', col_name)
    # Chuyển thành chữ thường
    cleaned = cleaned.lower()
    # Loại bỏ underscore ở đầu/cuối
    cleaned = cleaned.strip('_')
    # Đảm bảo tên cột không bắt đầu bằng số (thêm tiền tố nếu cần)
    if cleaned and cleaned[0].isdigit():
        cleaned = f"col_{cleaned}"
    # Xử lý trường hợp tên rỗng sau khi làm sạch
    if not cleaned:
        return "invalid_col_name"
    return cleaned

def get_duckdb_connection(db_path: str = ":memory:") -> Optional[duckdb.DuckDBPyConnection]:
    """Khởi tạo và trả về kết nối DuckDB."""
    logger.info(f"Đang kết nối đến DuckDB: {db_path}")
    try:
        con = duckdb.connect(database=db_path, read_only=False)
        logger.info("Kết nối DuckDB thành công.")
        return con
    except Exception as e:
        logger.error(f"Lỗi khi kết nối DuckDB: {e}", exc_info=True)
        return None

def create_table_from_csv_header(con: duckdb.DuckDBPyConnection, table_name: str, csv_path: Path) -> bool:
    """
    Tạo bảng DuckDB dựa trên header của file CSV và suy luận kiểu dữ liệu.
    """
    logger.info(f"Bắt đầu tạo bảng '{table_name}' từ header của {csv_path.name}")
    try:
        # Đọc dòng đầu tiên để lấy header
        df_header = pd.read_csv(csv_path, nrows=0) # Chỉ đọc header
        original_columns = df_header.columns.tolist()
        if not original_columns:
            logger.error("Không thể đọc header từ file CSV.")
            return False

        # Làm sạch tên cột
        cleaned_columns = [clean_col_name(col) for col in original_columns]
        logger.info(f"Tên cột gốc: {original_columns}")
        logger.info(f"Tên cột đã làm sạch: {cleaned_columns}")

        # Tạo câu lệnh CREATE TABLE với tên cột đã làm sạch
        # DuckDB có thể tự suy luận kiểu dữ liệu tốt từ CSV khi dùng read_csv_auto
        # Nhưng chúng ta sẽ tạo bảng trước để thực hành DDL
        # Tạo DDL dựa trên kiểu dữ liệu suy luận của pandas (cần đọc 1 phần dữ liệu)
        try:
            # Đọc vài dòng để pandas suy luận kiểu
            df_sample = pd.read_csv(csv_path, nrows=100)
            df_sample.columns = cleaned_columns # Áp dụng tên đã làm sạch
            # Tạo mapping kiểu pandas sang kiểu DuckDB (có thể cần điều chỉnh)
            type_mapping = {
                'int64': 'BIGINT', # Hoặc INTEGER nếu giá trị không quá lớn
                'float64': 'DOUBLE',
                'object': 'VARCHAR', # Kiểu string/text
                'datetime64[ns]': 'TIMESTAMP',
                'bool': 'BOOLEAN'
            }
            column_defs = []
            for col in cleaned_columns:
                pd_type = str(df_sample[col].dtype)
                duckdb_type = type_mapping.get(pd_type, 'VARCHAR') # Mặc định là VARCHAR
                # Điều chỉnh thêm nếu cần, ví dụ VIN có thể là VARCHAR cố định
                if col == 'vin_1_10': duckdb_type = 'VARCHAR(10)' # Giả sử VIN có độ dài cố định
                if col == 'state': duckdb_type = 'VARCHAR(2)'
                if col == 'postal_code': duckdb_type = 'VARCHAR(10)'
                if col == 'model_year': duckdb_type = 'INTEGER'
                if col == 'electric_range': duckdb_type = 'INTEGER'
                if col == 'base_msrp': duckdb_type = 'INTEGER' # Hoặc DOUBLE
                if col == 'dol_vehicle_id': duckdb_type = 'BIGINT'
                if col == '2020_census_tract': duckdb_type = 'BIGINT' # Đã làm sạch
                column_defs.append(f'"{col}" {duckdb_type}') # Đặt tên cột trong dấu ngoặc kép

            create_sql = f"CREATE OR REPLACE TABLE {table_name} ({', '.join(column_defs)});"
            logger.info(f"Đang thực thi DDL:\n{create_sql}")
            con.execute(create_sql)
            logger.info(f"Tạo bảng '{table_name}' thành công.")
            return True

        except Exception as e:
            logger.error(f"Lỗi khi suy luận kiểu dữ liệu hoặc tạo DDL: {e}", exc_info=True)
            return False

    except FileNotFoundError:
        logger.error(f"Không tìm thấy file CSV: {csv_path}")
        return False
    except Exception as e:
        logger.error(f"Lỗi khi đọc header hoặc tạo bảng '{table_name}': {e}", exc_info=True)
        return False


def load_csv_to_duckdb(con: duckdb.DuckDBPyConnection, table_name: str, csv_path: Path) -> bool:
    """Đọc dữ liệu từ CSV vào bảng DuckDB đã tạo."""
    logger.info(f"Bắt đầu nạp dữ liệu từ {csv_path.name} vào bảng {table_name}...")
    try:
        # Sử dụng chức năng đọc CSV mạnh mẽ của DuckDB
        # Nó tự động xử lý kiểu dữ liệu, dấu nháy, v.v.
        # header=true để bỏ qua dòng đầu
        # auto_detect=true để tự suy luận cấu trúc tốt hơn
        # ignore_errors=true để bỏ qua các dòng lỗi nếu có
        con.execute(f"""
            COPY {table_name} FROM '{str(csv_path)}'
            (HEADER, DELIMITER ',', QUOTE '"', ESCAPE '"', AUTO_DETECT TRUE, IGNORE_ERRORS TRUE);
        """)
        # Kiểm tra số lượng dòng đã nạp
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        logger.info(f"Nạp dữ liệu vào bảng {table_name} thành công. Tổng số dòng: {count[0] if count else 'N/A'}")
        return True
    except Exception as e:
        logger.error(f"Lỗi khi nạp dữ liệu CSV vào {table_name}: {e}", exc_info=True)
        return False

def run_analytics(con: duckdb.DuckDBPyConnection, table_name: str, reports_dir: Path):
    """Thực hiện các truy vấn phân tích và lưu/in kết quả."""
    logger.info("--- Bắt đầu chạy phân tích ---")
    try:
        # 3.1: Đếm xe theo thành phố
        logger.info("Câu 3.1: Đếm xe theo thành phố...")
        cars_per_city = con.execute(f"""
            SELECT city, COUNT(*) AS car_count
            FROM {table_name}
            WHERE city IS NOT NULL
            GROUP BY city
            ORDER BY car_count DESC;
        """).fetchdf() # Lấy kết quả dưới dạng Pandas DataFrame
        logger.info(f"Kết quả đếm xe theo thành phố (Top 10):\n{cars_per_city.head(10).to_string()}")
        # Có thể lưu ra file nếu muốn: cars_per_city.to_csv(reports_dir / "cars_per_city.csv", index=False)

        # 3.2: Top 3 mẫu xe phổ biến nhất
        logger.info("Câu 3.2: Top 3 mẫu xe phổ biến nhất...")
        top_3_models = con.execute(f"""
            SELECT make, model, COUNT(*) AS model_count
            FROM {table_name}
            WHERE make IS NOT NULL AND model IS NOT NULL
            GROUP BY make, model
            ORDER BY model_count DESC
            LIMIT 3;
        """).fetchdf()
        logger.info(f"Kết quả top 3 mẫu xe:\n{top_3_models.to_string()}")
        # top_3_models.to_csv(reports_dir / "top_3_models.csv", index=False)

        # 3.3: Mẫu xe phổ biến nhất theo mã bưu điện
        logger.info("Câu 3.3: Mẫu xe phổ biến nhất theo mã bưu điện...")
        popular_by_postal = con.execute(f"""
            WITH RankedModels AS (
                SELECT
                    postal_code,
                    make,
                    model,
                    COUNT(*) as model_count,
                    ROW_NUMBER() OVER(PARTITION BY postal_code ORDER BY COUNT(*) DESC) as rn
                FROM {table_name}
                WHERE postal_code IS NOT NULL AND make IS NOT NULL AND model IS NOT NULL
                GROUP BY postal_code, make, model
            )
            SELECT postal_code, make, model, model_count
            FROM RankedModels
            WHERE rn = 1
            ORDER BY postal_code
            LIMIT 20; -- Giới hạn output cho log
        """).fetchdf()
        logger.info(f"Kết quả mẫu xe phổ biến theo mã bưu điện (Top 20):\n{popular_by_postal.to_string()}")
        # Có thể lưu toàn bộ kết quả:
        # full_popular_by_postal = con.execute(... câu lệnh không có LIMIT ...).fetchdf()
        # full_popular_by_postal.to_csv(reports_dir / "popular_model_by_postal.csv", index=False)

        # 3.4: Đếm xe theo năm và lưu Parquet phân vùng
        logger.info("Câu 3.4: Đếm xe theo năm sản xuất và lưu Parquet...")
        output_parquet_path = reports_dir / "cars_by_year_partitioned"
        # Tạo thư mục reports nếu chưa có
        reports_dir.mkdir(parents=True, exist_ok=True)
        # Xóa thư mục cũ nếu tồn tại để tránh lỗi ghi đè
        if output_parquet_path.exists():
             logger.info(f"Xóa thư mục parquet cũ: {output_parquet_path}")
             shutil.rmtree(output_parquet_path)

        con.execute(f"""
            COPY (
                SELECT
                    model_year,
                    COUNT(*) AS car_count
                FROM {table_name}
                WHERE model_year IS NOT NULL
                GROUP BY model_year
                ORDER BY model_year
            ) TO '{str(output_parquet_path)}' (FORMAT PARQUET, PARTITION_BY (model_year));
        """)
        logger.info(f"Đã lưu kết quả số lượng xe theo năm vào thư mục Parquet: {output_parquet_path}")

        logger.info("--- Hoàn thành chạy phân tích ---")

    except Exception as e:
        logger.error(f"Lỗi trong quá trình phân tích: {e}", exc_info=True)


# --- Luồng Thực thi Chính ---
if __name__ == "__main__":
    logger.info("--- Bắt đầu Exercise 8: DuckDB ---")
    con = None
    try:
        # 1. Kết nối DuckDB
        con = get_duckdb_connection(DUCKDB_PATH)
        if not con:
            raise ConnectionError("Không thể kết nối đến DuckDB.")

        # 2. Tạo bảng từ header CSV
        if not create_table_from_csv_header(con, TABLE_NAME, INPUT_CSV_FILE):
             raise RuntimeError(f"Không thể tạo bảng '{TABLE_NAME}'.")

        # 3. Nạp dữ liệu từ CSV vào bảng
        if not load_csv_to_duckdb(con, TABLE_NAME, INPUT_CSV_FILE):
            raise RuntimeError("Không thể nạp dữ liệu CSV vào DuckDB.")

        # 4. Chạy các truy vấn phân tích
        run_analytics(con, TABLE_NAME, REPORTS_DIR)

    except Exception as e:
         logger.error(f"Pipeline thất bại: {e}", exc_info=True)
         exit(1)
    finally:
        # Đóng kết nối DuckDB
        if con:
            con.close()
            logger.info("Đã đóng kết nối DuckDB.")

    logger.info("--- Kết thúc Exercise 8 ---")

