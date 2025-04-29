import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from pathlib import Path
from urllib.parse import urljoin
import io
from typing import Optional
# import time # Không cần time nữa
import re

# --- Cấu hình Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- Hằng số ---
BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
# Timestamp mục tiêu, sẽ được làm sạch trước khi so sánh
TARGET_TIMESTAMP_RAW = "2024-01-19 10:27 "
DOWNLOAD_DIR = Path("downloaded_weather_data")
TARGET_COLUMN = "HourlyDryBulbTemperature"
# Bỏ các hằng số retry
# MAX_FETCH_RETRIES = 3
# FETCH_RETRY_DELAY = 10

# --- Các Hàm Hỗ trợ ---

def fetch_html(url: str) -> Optional[str]:
    """Lấy nội dung HTML từ URL (không có retry)."""
    # <<< LOẠI BỎ VÒNG LẶP WHILE VÀ LOGIC RETRY >>>
    logger.info(f"Đang lấy HTML từ: {url}")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status() # Vẫn kiểm tra lỗi HTTP (4xx, 5xx)
        logger.info("Lấy HTML thành công.")
        response.encoding = 'utf-8' # Đảm bảo đúng encoding
        return response.text
    except requests.exceptions.HTTPError as e:
        # Log lỗi HTTP cụ thể
        logger.error(f"Lỗi HTTP {e.response.status_code} khi lấy HTML từ {url}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        # Log các lỗi request khác
        logger.error(f"Lỗi request khác khi lấy HTML từ {url}: {e}")
        return None
    # Không cần log "Không thể lấy HTML sau ... lần thử" nữa

def find_file_url_by_timestamp(html_content: str, base_url: str, target_timestamp_raw: str) -> Optional[str]:
    """Phân tích HTML, tìm tên file dựa vào timestamp và trả về URL đầy đủ."""
    # (Giữ nguyên hàm này như phiên bản lab9_ex2_main_py_final_robust)
    if not html_content:
        logger.error("Không có nội dung HTML để phân tích.")
        return None

    target_ts_cleaned = " ".join(target_timestamp_raw.split())
    logger.info(f"Đang tìm file với timestamp (đã làm sạch): '{target_ts_cleaned}'")

    soup = BeautifulSoup(html_content, 'lxml')
    table = soup.find('table')
    if not table:
        logger.error("Không tìm thấy thẻ <table> nào trong HTML.")
        return None
    table_rows = table.find_all('tr')

    if not table_rows or len(table_rows) <= 1:
        logger.error("Không tìm thấy dòng dữ liệu (tr) nào trong bảng.")
        return None

    found_url = None
    for row in table_rows[1:]:
        cells = row.find_all('td')
        if len(cells) >= 3:
            try:
                last_modified_raw_html = cells[1].get_text()
                last_modified_no_nbsp = last_modified_raw_html.replace('\xa0', ' ')
                last_modified_cleaned_html = " ".join(last_modified_no_nbsp.split())

                if last_modified_cleaned_html == target_ts_cleaned:
                    link_tag = cells[0].find('a')
                    if link_tag and link_tag.has_attr('href'):
                        filename = link_tag['href']
                        found_url = urljoin(base_url, filename)
                        logger.info(f"Đã tìm thấy file: {filename} ứng với timestamp '{last_modified_raw_html}'. URL: {found_url}")
                        break
            except IndexError:
                logger.debug(f"Bỏ qua dòng không đủ cột hoặc cấu trúc khác: {row}")
                continue
            except Exception as e:
                logger.warning(f"Lỗi khi xử lý dòng: {row}. Lỗi: {e}")
                continue

    if not found_url:
        logger.error(f"Không tìm thấy file nào khớp với timestamp '{target_ts_cleaned}'.")

    return found_url

def download_and_process_file(file_url: str, target_column: str):
    """Tải file CSV, đọc bằng Pandas và tìm giá trị lớn nhất."""
    # (Giữ nguyên hàm này như phiên bản lab9_ex2_main_py_final_robust)
    logger.info(f"Đang tải và xử lý file từ: {file_url}")
    try:
        response = requests.get(file_url, timeout=120)
        response.raise_for_status()
        logger.info("Tải file thành công.")

        csv_content = response.text
        df = pd.read_csv(io.StringIO(csv_content), low_memory=False)
        logger.info(f"Đọc file CSV thành công. Shape: {df.shape}")

        if target_column not in df.columns:
            logger.error(f"Không tìm thấy cột '{target_column}' trong file CSV.")
            logger.info(f"Các cột có sẵn: {list(df.columns)}")
            return

        df[target_column] = df[target_column].astype(str).str.replace(r'[^\d.-]+', '', regex=True)
        df[target_column] = pd.to_numeric(df[target_column], errors='coerce')


        if df[target_column].isnull().all():
            logger.error(f"Cột '{target_column}' không chứa giá trị số hợp lệ sau khi chuyển đổi.")
            return

        max_temp = df[target_column].max()
        logger.info(f"Giá trị {target_column} lớn nhất tìm thấy: {max_temp}")

        highest_temp_records = df[df[target_column] == max_temp]

        logger.info(f"--- Các bản ghi có {target_column} cao nhất ({max_temp}) ---")
        print(highest_temp_records.to_string())
        logger.info(f"--- Kết thúc in bản ghi ---")

    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi khi tải file {file_url}: {e}")
    except pd.errors.EmptyDataError:
         logger.error(f"File CSV tải về từ {file_url} bị rỗng.")
    except Exception as e:
        logger.error(f"Lỗi khi xử lý file CSV hoặc tìm giá trị lớn nhất: {e}", exc_info=True)


# --- Luồng Thực thi Chính ---
if __name__ == "__main__":
    logger.info("--- Bắt đầu Exercise 2: Web Scraping và Phân tích Dữ liệu ---")

    html = fetch_html(BASE_URL) # Gọi hàm fetch đã tối ưu

    if html:
        target_file_url = find_file_url_by_timestamp(html, BASE_URL, TARGET_TIMESTAMP_RAW)

        if target_file_url:
            download_and_process_file(target_file_url, TARGET_COLUMN)
        else:
            logger.error("Không thể tìm thấy URL của file cần tải.")
            exit(1)
    else:
        logger.error("Không thể lấy nội dung trang web. Dừng script.")
        exit(1)

    logger.info("--- Kết thúc Exercise 2 ---")
