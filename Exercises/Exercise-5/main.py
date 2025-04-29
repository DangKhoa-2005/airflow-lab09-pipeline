import psycopg2
import os
import logging
import csv
import pandas as pd
from pathlib import Path
from typing import Optional, List, Tuple
from io import StringIO

# --- Cấu hình Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- Lấy thông tin kết nối DB từ biến môi trường ---
db_host = os.getenv("DB_HOST", "localhost")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("DB_NAME", "mydatabase")
db_user = os.getenv("DB_USER", "user")
db_password = os.getenv("DB_PASSWORD", "password")

# --- Đường dẫn ---
DATA_DIR = Path("data")
SCHEMA_FILE = Path("schema.sql")
ACCOUNTS_CSV = DATA_DIR / "accounts.csv"
PRODUCTS_CSV = DATA_DIR / "products.csv"
TRANSACTIONS_CSV = DATA_DIR / "transactions.csv"

# --- Các Hàm Hỗ trợ ---

def get_db_connection() -> Optional[psycopg2.extensions.connection]:
    """Thiết lập kết nối đến PostgreSQL."""
    conn_string = f"host='{db_host}' port='{db_port}' dbname='{db_name}' user='{db_user}' password='{db_password}'"
    try:
        logger.info(f"Đang kết nối đến PostgreSQL: host={db_host} port={db_port} dbname={db_name}")
        conn = psycopg2.connect(conn_string)
        logger.info("Kết nối PostgreSQL thành công.")
        conn.autocommit = False
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Lỗi kết nối PostgreSQL: {e}")
        return None
    except Exception as e:
        logger.error(f"Lỗi không xác định khi kết nối PostgreSQL: {e}")
        return None

def execute_sql_script(conn: psycopg2.extensions.connection, filepath: Path) -> bool:
    """Đọc và thực thi các lệnh SQL từ một file."""
    if not filepath.is_file():
        logger.error(f"Không tìm thấy file schema SQL: {filepath}")
        return False
    try:
        logger.info(f"Đang thực thi script SQL từ: {filepath}")
        with filepath.open('r', encoding='utf-8') as f:
            sql_script = f.read()
        with conn.cursor() as cur:
            cur.execute(sql_script)
        conn.commit()
        logger.info("Thực thi script SQL thành công.")
        return True
    except psycopg2.Error as e:
        logger.error(f"Lỗi khi thực thi script SQL: {e}")
        conn.rollback()
        return False
    except Exception as e:
        logger.error(f"Lỗi không xác định khi thực thi script SQL: {e}")
        conn.rollback()
        return False

def ingest_csv_to_table(conn: psycopg2.extensions.connection, csv_path: Path, table_name: str) -> bool:
    """Nạp dữ liệu từ file CSV vào bảng PostgreSQL dùng copy_expert."""
    if not csv_path.is_file():
        logger.error(f"Không tìm thấy file CSV: {csv_path}")
        return False

    logger.info(f"Bắt đầu nạp dữ liệu từ {csv_path.name} vào bảng {table_name}...")
    try:
        # Đọc CSV bằng pandas
        df = pd.read_csv(csv_path)
        logger.info(f"Đã đọc {len(df)} dòng từ {csv_path.name}")

        if df.empty:
            logger.warning(f"File {csv_path.name} rỗng, không có dữ liệu để nạp.")
            return True

        # <<< THAY ĐỔI QUAN TRỌNG: Làm sạch tên cột >>>
        # Loại bỏ khoảng trắng thừa ở đầu/cuối mỗi tên cột
        df.columns = [col.strip() for col in df.columns]
        logger.info(f"Tên cột sau khi làm sạch: {list(df.columns)}")
        # ------------------------------------------

        # Tạo buffer string chứa dữ liệu CSV (không header)
        buffer = StringIO()
        # Giả sử file CSV dùng dấu phẩy
        df.to_csv(buffer, index=False, header=False, sep=',', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        buffer.seek(0)

        # Lấy danh sách tên cột đã làm sạch
        # Đặt tên cột trong dấu ngoặc kép
        columns = ', '.join([f'"{col}"' for col in df.columns]) # Sử dụng tên cột đã strip()

        # Dùng copy_expert
        with conn.cursor() as cur:
            # Cập nhật lệnh COPY cho phù hợp với dấu phân cách
            copy_sql = f"COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',', HEADER FALSE, NULL '', QUOTE '\"', ESCAPE '\\')"
            logger.debug(f"Executing COPY command for {table_name}")
            cur.copy_expert(sql=copy_sql, file=buffer)
        conn.commit()
        logger.info(f"Nạp dữ liệu vào bảng {table_name} thành công.")
        return True

    except pd.errors.EmptyDataError:
         logger.warning(f"File {csv_path.name} rỗng hoặc chỉ có header.")
         return True
    except psycopg2.Error as e:
        logger.error(f"Lỗi PostgreSQL khi nạp dữ liệu vào {table_name}: {e}")
        # Kiểm tra xem lỗi có phải do tên cột không khớp không
        if "column" in str(e) and "does not exist" in str(e):
             logger.error(f"Kiểm tra lại tên cột trong file CSV '{csv_path.name}' và bảng '{table_name}' trong schema.sql.")
        conn.rollback()
        return False
    except FileNotFoundError:
         logger.error(f"Pandas không tìm thấy file CSV: {csv_path}")
         return False
    except Exception as e:
        logger.error(f"Lỗi không xác định khi nạp dữ liệu vào {table_name}: {e}", exc_info=True)
        conn.rollback()
        return False

# --- Luồng Thực thi Chính ---
if __name__ == "__main__":
    logger.info("--- Bắt đầu Exercise 5: Data Modeling và Ingestion ---")

    conn = None
    all_success = True

    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Không thể kết nối đến database.")

        if not execute_sql_script(conn, SCHEMA_FILE):
            raise Exception("Không thể tạo bảng từ schema.sql.")

        logger.info("--- Bắt đầu nạp dữ liệu ---")
        if not ingest_csv_to_table(conn, ACCOUNTS_CSV, "accounts"):
            all_success = False
        if not ingest_csv_to_table(conn, PRODUCTS_CSV, "products"):
            all_success = False
        if not ingest_csv_to_table(conn, TRANSACTIONS_CSV, "transactions"):
            all_success = False
        logger.info("--- Kết thúc nạp dữ liệu ---")


    except Exception as e:
        logger.error(f"Pipeline thất bại: {e}")
        all_success = False
    finally:
        if conn:
            conn.close()
            logger.info("Đã đóng kết nối PostgreSQL.")

    logger.info("--- Kết thúc Exercise 5 ---")
    if not all_success:
        exit(1)
