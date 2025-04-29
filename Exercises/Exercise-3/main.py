import requests
import gzip
import io
import logging
from typing import Optional

# --- Cấu hình Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- Hằng số ---
# Sử dụng endpoint HTTP công khai khác
BASE_URL = "https://data.commoncrawl.org/"
PATHS_FILE_KEY = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"
PATHS_FILE_URL = f"{BASE_URL}{PATHS_FILE_KEY}"

LINES_TO_PRINT = 100 # Số dòng tối đa muốn in ra từ file WET

# --- Các Hàm Hỗ trợ ---

def download_gz_content_in_memory(url: str) -> Optional[bytes]:
    """Tải file .gz từ URL và trả về nội dung đã giải nén trong bộ nhớ."""
    logger.info(f"Đang tải và giải nén file từ: {url}")
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status() # Kiểm tra lỗi HTTP
        logger.info("Tải file .gz thành công.")

        # Giải nén nội dung gzip trong bộ nhớ
        # Sử dụng hàm decompress_gz_content đã định nghĩa bên dưới
        decompressed_content = decompress_gz_content(response.content)
        if decompressed_content:
             logger.info("Giải nén file .gz trong bộ nhớ thành công.")
             return decompressed_content
        else:
             # Lỗi đã được log bên trong hàm decompress_gz_content
             return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi khi tải file {url}: {e}")
        return None
    # Không cần bắt lỗi gzip ở đây nữa vì đã gọi hàm riêng
    except Exception as e:
        logger.error(f"Lỗi không xác định khi tải {url}: {e}")
        return None

# <<< HÀM BỊ THIẾU ĐÃ ĐƯỢC THÊM VÀO >>>
def decompress_gz_content(gzipped_content: bytes) -> Optional[bytes]:
    """Giải nén nội dung gzip trong bộ nhớ."""
    if not gzipped_content:
        return None
    logger.info("Đang giải nén nội dung .gz trong bộ nhớ...")
    try:
        # Sử dụng BytesIO để đọc content như file
        with gzip.GzipFile(fileobj=io.BytesIO(gzipped_content), mode='rb') as f:
            decompressed_content = f.read()
        logger.info("Giải nén thành công.")
        return decompressed_content
    except gzip.BadGzipFile:
        logger.error("Nội dung không phải định dạng gzip hợp lệ.")
        return None
    except Exception as e:
        logger.error(f"Lỗi khi giải nén nội dung gzip: {e}")
        return None

def get_first_line(content: bytes) -> Optional[str]:
    """Lấy dòng đầu tiên từ nội dung bytes (đã giải nén)."""
    # (Giữ nguyên hàm này)
    if not content:
        return None
    try:
        lines = content.decode('utf-8').splitlines()
        if lines:
            first_line = lines[0].strip()
            logger.info(f"Dòng đầu tiên (key file WET): {first_line}")
            return first_line
        else:
            logger.warning("Nội dung file paths rỗng sau khi giải nén.")
            return None
    except UnicodeDecodeError:
        logger.error("Không thể giải mã nội dung file paths bằng UTF-8.")
        return None
    except Exception as e:
        logger.error(f"Lỗi khi đọc dòng đầu tiên: {e}")
        return None

def stream_and_print_wet_file(wet_file_key: str, num_lines: int):
    """Tải, giải nén và in N dòng đầu tiên của file WET."""
    # (Giữ nguyên hàm này)
    wet_file_url = f"{BASE_URL}{wet_file_key}"
    logger.info(f"Đang xử lý file WET từ: {wet_file_url}")
    lines_printed = 0
    try:
        with requests.get(wet_file_url, stream=True, timeout=300) as response:
            response.raise_for_status()
            logger.info("Bắt đầu stream và giải nén file WET...")

            gzip_stream = gzip.GzipFile(fileobj=io.BufferedReader(response.raw), mode='rb')
            text_stream = io.TextIOWrapper(gzip_stream, encoding='utf-8', errors='ignore')

            for line in text_stream:
                if lines_printed < num_lines:
                    print(line.strip())
                    lines_printed += 1
                else:
                    logger.info(f"Đã in {num_lines} dòng đầu tiên. Dừng xử lý.")
                    break

            if lines_printed < num_lines:
                 logger.info(f"Đã xử lý hết file WET. Tổng số dòng đã in: {lines_printed}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi khi tải file WET {wet_file_url}: {e}")
    except gzip.BadGzipFile:
        logger.error(f"File WET tải về từ {wet_file_url} không phải định dạng gzip hợp lệ.")
    except Exception as e:
        logger.error(f"Lỗi không xác định khi xử lý file WET {wet_file_url}: {e}", exc_info=True)


# --- Luồng Thực thi Chính ---
if __name__ == "__main__":
    logger.info("--- Bắt đầu Exercise 3: Tải file bằng Requests (data.commoncrawl.org) ---")

    # 1. Tải nội dung file paths vào bộ nhớ (hàm này giờ gọi hàm giải nén)
    paths_content_gz = download_gz_content_in_memory(PATHS_FILE_URL) # Đổi tên biến để rõ hơn

    # Kiểm tra kết quả tải và giải nén file paths
    if paths_content_gz: # Hàm download_gz_content_in_memory giờ trả về nội dung đã giải nén
        paths_content_decompressed = paths_content_gz # Đổi tên biến cho rõ nghĩa
        # 3. Lấy key của file WET từ dòng đầu tiên
        wet_file_key = get_first_line(paths_content_decompressed)

        if wet_file_key:
            # 4. & 5. Stream, giải nén và in N dòng đầu của file WET
            stream_and_print_wet_file(wet_file_key, LINES_TO_PRINT)
        else:
            logger.error("Không thể lấy được key của file WET từ file paths.")
            exit(1)
    else:
        logger.error("Không thể tải hoặc giải nén file paths.")
        exit(1)

    logger.info("--- Kết thúc Exercise 3 ---")
