import requests
import zipfile
import os
from pathlib import Path
from urllib.parse import urlparse
import logging
import shutil # Để xóa thư mục __MACOSX nếu có
from typing import List, Optional # Thêm typing

# --- Cấu hình Logging ---
# Thiết lập logging để ghi thông tin ra console
logging.basicConfig(
    level=logging.INFO, # Mức độ log (INFO, DEBUG, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', # Định dạng log
    handlers=[logging.StreamHandler()] # Gửi log ra standard output (console)
)
# Lấy logger cho module hiện tại
logger = logging.getLogger(__name__)

# --- Danh sách URI cần tải ---
# !!! QUAN TRỌNG: Bạn PHẢI thay thế danh sách này bằng các URL thực tế !!!
# !!! có trong file main.py gốc từ repository bạn đã fork trên GitHub.   !!!
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

# --- Đường dẫn Thư mục ---
# Định nghĩa thư mục để lưu file tải về và giải nén
# Path("downloads") tạo một đối tượng Path đại diện cho thư mục 'downloads'
# trong thư mục hiện tại nơi script chạy.
DOWNLOADS_DIR: Path = Path("downloads")

# --- Các Hàm Hỗ trợ ---

def create_download_dir(dir_path: Path):
    """
    Tạo thư mục nếu nó chưa tồn tại.

    Args:
        dir_path (Path): Đối tượng Path đại diện cho thư mục cần tạo.
    """
    try:
        # Tạo thư mục.
        # parents=True: Tạo luôn các thư mục cha nếu cần.
        # exist_ok=True: Không báo lỗi nếu thư mục đã tồn tại.
        dir_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Thư mục '{dir_path}' đã được tạo hoặc đã tồn tại.")
    except OSError as e:
        # Bắt lỗi hệ điều hành (ví dụ: không có quyền ghi)
        logger.error(f"Không thể tạo thư mục '{dir_path}': {e}")
        raise # Ném lại lỗi để dừng chương trình nếu không tạo được thư mục thiết yếu

def get_filename_from_uri(uri: str) -> Optional[str]:
    """
    Trích xuất tên file từ một URI (đường dẫn URL).

    Args:
        uri (str): Chuỗi URI cần phân tích.

    Returns:
        Optional[str]: Tên file nếu trích xuất thành công, None nếu thất bại.
    """
    try:
        # Phân tích URI thành các thành phần (scheme, netloc, path, params, query, fragment)
        parsed_uri = urlparse(uri)
        # Lấy phần đường dẫn (path) từ URI
        path = parsed_uri.path
        # Trích xuất phần tên file cuối cùng từ đường dẫn
        filename = os.path.basename(path)
        # Kiểm tra xem có lấy được tên file không
        if not filename:
            logger.warning(f"Không thể trích xuất tên file từ URI: {uri}")
            return None
        return filename
    except Exception as e:
        # Bắt các lỗi khác có thể xảy ra trong quá trình phân tích
        logger.error(f"Lỗi khi trích xuất tên file từ URI '{uri}': {e}")
        return None

def download_file(uri: str, save_path: Path) -> bool:
    """
    Tải file từ URI và lưu vào đường dẫn chỉ định.

    Args:
        uri (str): URI của file cần tải.
        save_path (Path): Đường dẫn đầy đủ để lưu file tải về.

    Returns:
        bool: True nếu tải và lưu thành công, False nếu có lỗi.
    """
    logger.info(f"Đang tải file từ: {uri}")
    try:
        # Gửi yêu cầu GET đến URI
        # stream=True: Tải dữ liệu theo từng phần nhỏ, hiệu quả cho file lớn.
        # timeout=60: Đặt thời gian chờ tối đa 60 giây cho request.
        response = requests.get(uri, stream=True, timeout=60)
        # Kiểm tra xem request có thành công không (status code 2xx)
        # Nếu không thành công (vd: 404 Not Found, 500 Server Error), sẽ ném ra HTTPError.
        response.raise_for_status()

        # Mở file tại đường dẫn lưu ở chế độ ghi nhị phân ('wb')
        with save_path.open('wb') as f:
            # Lặp qua từng phần dữ liệu (chunk) tải về
            for chunk in response.iter_content(chunk_size=8192): # Kích thước chunk 8KB
                # Ghi chunk vào file
                f.write(chunk)
        logger.info(f"Tải thành công và lưu vào: {save_path}")
        return True
    except requests.exceptions.Timeout:
        logger.error(f"Timeout khi tải file từ: {uri}")
        return False
    except requests.exceptions.HTTPError as e:
        logger.error(f"Lỗi HTTP {e.response.status_code} khi tải file từ {uri}: {e}")
        return False
    except requests.exceptions.RequestException as e:
        # Các lỗi request khác (vd: lỗi DNS, lỗi kết nối)
        logger.error(f"Lỗi request khi tải file từ {uri}: {e}")
        return False
    except IOError as e:
        # Lỗi khi ghi file xuống đĩa
        logger.error(f"Lỗi I/O khi lưu file {save_path}: {e}")
        return False
    except Exception as e:
        # Bắt các lỗi không lường trước khác
        logger.error(f"Lỗi không xác định khi tải/lưu file {uri}: {e}")
        return False

def unzip_file(zip_path: Path, extract_to: Path) -> bool:
    """
    Giải nén file zip và xóa file zip gốc nếu thành công.

    Args:
        zip_path (Path): Đường dẫn đến file zip cần giải nén.
        extract_to (Path): Thư mục đích để giải nén file vào.

    Returns:
        bool: True nếu giải nén thành công, False nếu có lỗi.
    """
    logger.info(f"Đang giải nén file: {zip_path}")
    try:
        # Mở file zip ở chế độ đọc ('r')
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Lấy danh sách tất cả các file/thư mục bên trong zip
            file_list = zip_ref.namelist()
            # Kiểm tra xem có file CSV nào không (không phân biệt hoa thường)
            csv_files = [name for name in file_list if name.lower().endswith('.csv')]

            if not csv_files:
                logger.warning(f"Không tìm thấy file CSV nào trong {zip_path.name}")
                # Quyết định không xóa file zip nếu không có CSV bên trong
                return False # Coi như thất bại vì không có CSV
            else:
                 # Giải nén tất cả nội dung vào thư mục đích
                 zip_ref.extractall(extract_to)
                 logger.info(f"Giải nén thành công vào: {extract_to}")

                 # Xóa file zip sau khi giải nén thành công
                 try:
                     os.remove(zip_path)
                     logger.info(f"Đã xóa file zip: {zip_path.name}")
                 except OSError as e:
                     # Ghi log lỗi nếu không xóa được nhưng vẫn coi là giải nén thành công
                     logger.error(f"Không thể xóa file zip {zip_path.name}: {e}")

            # Xử lý thư mục __MACOSX (thường do tạo zip trên macOS)
            macosx_dir = extract_to / "__MACOSX"
            if macosx_dir.is_dir():
                try:
                    shutil.rmtree(macosx_dir) # Xóa thư mục và toàn bộ nội dung
                    logger.info(f"Đã xóa thư mục không cần thiết: {macosx_dir}")
                except OSError as e:
                    logger.error(f"Không thể xóa thư mục {macosx_dir}: {e}")

        return True # Giải nén thành công (dù có thể không xóa được zip)
    except zipfile.BadZipFile:
        # Lỗi nếu file không phải định dạng zip hợp lệ
        logger.error(f"File không hợp lệ hoặc không phải file zip: {zip_path.name}")
        return False
    except Exception as e:
        # Bắt các lỗi khác trong quá trình giải nén
        logger.error(f"Lỗi khi giải nén file {zip_path.name}: {e}")
        return False

# --- Luồng Thực thi Chính ---
if __name__ == "__main__":
    logger.info("--- Bắt đầu Exercise 1: Tải và Giải nén Files ---")

    # 1. Đảm bảo thư mục downloads tồn tại
    create_download_dir(DOWNLOADS_DIR)

    # Biến đếm kết quả
    success_count = 0
    fail_count = 0

    # Lặp qua từng URI trong danh sách
    for uri in download_uris:
        logger.info(f"--- Xử lý URI: {uri} ---")

        # 3. Lấy tên file từ URI
        filename = get_filename_from_uri(uri)
        if not filename:
            # Nếu không lấy được tên file, ghi lỗi và bỏ qua URI này
            logger.error(f"Bỏ qua URI do không lấy được tên file: {uri}")
            fail_count += 1
            continue # Chuyển sang URI tiếp theo

        # Tạo đường dẫn đầy đủ để lưu file zip
        zip_save_path = DOWNLOADS_DIR / filename

        # 2. Thực hiện tải file
        download_successful = download_file(uri, zip_save_path)

        # Chỉ giải nén nếu tải thành công
        if download_successful:
            # 4. Giải nén file và xóa file zip nếu thành công
            unzip_successful = unzip_file(zip_save_path, DOWNLOADS_DIR)
            if unzip_successful:
                # Tăng biến đếm thành công nếu cả tải và giải nén đều ổn
                success_count += 1
            else:
                # Nếu giải nén lỗi, tăng biến đếm thất bại
                logger.error(f"Giải nén thất bại cho: {filename}. File zip có thể vẫn còn.")
                fail_count += 1
        else:
            # Nếu tải file thất bại, tăng biến đếm thất bại
            fail_count += 1

    # In tổng kết sau khi xử lý hết các URI
    logger.info("--- Kết thúc Exercise 1 ---")
    logger.info(f"Tổng kết: {success_count} file xử lý thành công, {fail_count} file thất bại.")

