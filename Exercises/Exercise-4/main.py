import json
import csv
import logging
from pathlib import Path
import pandas as pd # Sử dụng pandas để làm phẳng và ghi CSV dễ dàng hơn
from typing import Dict, Any, List, Optional

# --- Cấu hình Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- Hằng số ---
# Thư mục chứa dữ liệu JSON (bên trong thư mục Exercise-4,
# sẽ được mount vào /app/data trong container)
DATA_DIR = Path("data")
# Thư mục để lưu file CSV output (có thể cùng thư mục data hoặc khác)
# Ở đây lưu cùng chỗ với file JSON gốc
OUTPUT_DIR = Path("data")

# --- Các Hàm Hỗ trợ ---

def find_json_files(search_dir: Path) -> List[Path]:
    """Tìm tất cả các file .json trong thư mục và các thư mục con."""
    logger.info(f"Đang tìm kiếm file .json trong thư mục: {search_dir}")
    # search_dir.rglob('*.json') tìm kiếm đệ quy tất cả file có đuôi .json
    json_files = list(search_dir.rglob('*.json'))
    logger.info(f"Tìm thấy {len(json_files)} file JSON.")
    if not json_files:
        logger.warning(f"Không tìm thấy file JSON nào trong {search_dir}")
    return json_files

def flatten_json_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Làm phẳng một bản ghi JSON, xử lý trường 'geometry' đặc biệt.

    Ví dụ: {"type": "Point", "coordinates": [-99.9, 16.88333]}
    sẽ thành: {"geometry_type": "Point", "geometry_coordinates_0": -99.9, "geometry_coordinates_1": 16.88333}

    Args:
        record (Dict[str, Any]): Một dictionary đại diện cho bản ghi JSON.

    Returns:
        Dict[str, Any]: Dictionary đã được làm phẳng.
    """
    flat_record = {}
    for key, value in record.items():
        if isinstance(value, dict):
            # Xử lý trường hợp đặc biệt của 'geometry'
            if key == 'geometry' and 'type' in value and 'coordinates' in value and isinstance(value['coordinates'], list):
                flat_record['geometry_type'] = value.get('type')
                # Tạo các cột riêng cho từng tọa độ
                for i, coord in enumerate(value['coordinates']):
                    flat_record[f'geometry_coordinates_{i}'] = coord
            else:
                # Làm phẳng các dictionary khác (nếu có) bằng cách thêm tiền tố key
                # Ví dụ: {"properties": {"mag": 5.0}} -> {"properties.mag": 5.0}
                # Dùng pandas.json_normalize sẽ xử lý tốt hơn, nhưng đây là cách thủ công
                # for sub_key, sub_value in value.items():
                #     flat_record[f"{key}.{sub_key}"] = sub_value
                # Tạm thời chỉ giữ lại giá trị dạng string nếu không phải geometry
                 flat_record[key] = str(value) # Chuyển dict khác thành string để tránh lỗi
        elif isinstance(value, list):
             # Chuyển list thành chuỗi để đưa vào CSV, hoặc xử lý phức tạp hơn nếu cần
             flat_record[key] = json.dumps(value) # Lưu list dưới dạng chuỗi JSON
        else:
            # Giữ nguyên các giá trị khác
            flat_record[key] = value
    return flat_record

def process_json_file(json_path: Path, output_dir: Path):
    """Đọc file JSON, làm phẳng và ghi ra file CSV."""
    logger.info(f"Đang xử lý file: {json_path}")
    try:
        # Đọc file JSON bằng pandas
        # Giả định file JSON chứa một danh sách các object JSON
        # hoặc một object JSON duy nhất trên mỗi dòng (lines=True)
        # Dựa vào cấu trúc repo, có vẻ mỗi file là một JSON lớn chứa list trong key 'features'
        with json_path.open('r', encoding='utf-8') as f:
            data = json.load(f)

        # Kiểm tra cấu trúc dữ liệu JSON phổ biến của GeoJSON
        records_to_process: List[Dict[str, Any]] = []
        if isinstance(data, dict) and data.get('type') == 'FeatureCollection' and 'features' in data:
            logger.info("Phát hiện cấu trúc GeoJSON FeatureCollection. Đang xử lý các 'features'.")
            # Mỗi feature là một record, cần lấy cả properties và geometry
            for feature in data.get('features', []):
                if isinstance(feature, dict):
                    record = feature.get('properties', {}) # Lấy các properties
                    geometry = feature.get('geometry')
                    if isinstance(geometry, dict):
                         record['geometry'] = geometry # Thêm geometry vào record để flatten
                    records_to_process.append(record)
        elif isinstance(data, list):
             logger.info("File JSON chứa một danh sách các bản ghi.")
             records_to_process = data
        elif isinstance(data, dict):
             logger.info("File JSON chứa một bản ghi duy nhất.")
             records_to_process = [data] # Coi như list có 1 phần tử
        else:
            logger.error(f"Định dạng JSON không được hỗ trợ trong file: {json_path}")
            return False

        if not records_to_process:
             logger.warning(f"Không có bản ghi nào để xử lý trong file: {json_path}")
             return True # Coi như thành công vì file rỗng hoặc không có feature

        # Làm phẳng từng bản ghi
        flattened_data = [flatten_json_record(record) for record in records_to_process]

        if not flattened_data:
             logger.warning(f"Không có dữ liệu sau khi làm phẳng từ file: {json_path}")
             return True

        # Tạo DataFrame từ dữ liệu đã làm phẳng
        df_flattened = pd.DataFrame(flattened_data)

        # Tạo đường dẫn file CSV output
        # Đổi đuôi file từ .json thành .csv
        csv_filename = json_path.stem + ".csv"
        output_path = output_dir / csv_filename

        # Ghi DataFrame ra file CSV
        output_path.parent.mkdir(parents=True, exist_ok=True) # Đảm bảo thư mục output tồn tại
        df_flattened.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Đã ghi thành công file CSV: {output_path}")
        return True

    except json.JSONDecodeError as e:
        logger.error(f"Lỗi giải mã JSON trong file {json_path}: {e}")
        return False
    except FileNotFoundError:
        logger.error(f"Không tìm thấy file {json_path}")
        return False
    except KeyError as e:
         logger.error(f"Thiếu key mong đợi '{e}' trong cấu trúc JSON của file {json_path}")
         return False
    except Exception as e:
        logger.error(f"Lỗi không xác định khi xử lý file {json_path}: {e}", exc_info=True)
        return False

# --- Luồng Thực thi Chính ---
if __name__ == "__main__":
    logger.info("--- Bắt đầu Exercise 4: Chuyển đổi JSON sang CSV ---")

    # 1. Tìm tất cả các file JSON trong thư mục data
    json_files_to_process = find_json_files(DATA_DIR)

    if not json_files_to_process:
        logger.warning("Không tìm thấy file JSON nào để xử lý. Kết thúc.")
        exit(0) # Kết thúc bình thường nếu không có file

    success_count = 0
    fail_count = 0

    # 2. & 3. & 4. Lặp qua từng file, đọc, làm phẳng và ghi CSV
    for json_file in json_files_to_process:
        if process_json_file(json_file, OUTPUT_DIR):
            success_count += 1
        else:
            fail_count += 1

    logger.info("--- Kết thúc Exercise 4 ---")
    logger.info(f"Tổng kết: {success_count} file xử lý thành công, {fail_count} file thất bại.")

    # Thoát với mã lỗi nếu có file thất bại
    if fail_count > 0:
        exit(1)

