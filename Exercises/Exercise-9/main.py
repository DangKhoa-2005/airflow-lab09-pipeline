import polars as pl
import logging
from pathlib import Path
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
INPUT_CSV_FILE = DATA_DIR / "202306-divvy-tripdata.csv" # Kiểm tra lại tên file nếu cần

# --- Các Hàm Xử lý ---

def read_csv_lazy(filepath: Path) -> Optional[pl.LazyFrame]:
    """Đọc file CSV vào Polars LazyFrame."""
    if not filepath.is_file():
        logger.error(f"Không tìm thấy file CSV: {filepath}")
        return None
    logger.info(f"Bắt đầu quét file CSV (lazy): {filepath}")
    try:
        lf = pl.scan_csv(filepath, try_parse_dates=True, low_memory=True)
        logger.info("Quét file CSV thành công vào LazyFrame.")
        # Không cần lấy schema ở đây nữa để tránh warning/potential cost
        # logger.info(f"Schema suy luận ban đầu:\n{lf.schema}")
        return lf
    except Exception as e:
        logger.error(f"Lỗi khi quét file CSV {filepath}: {e}", exc_info=True)
        return None

def convert_datatypes(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Chuyển đổi kiểu dữ liệu cho các cột cần thiết."""
    logger.info("Bắt đầu chuyển đổi kiểu dữ liệu...")
    dtype_mapping = {
        "started_at": pl.Datetime,
        "ended_at": pl.Datetime,
        "start_station_id": pl.Utf8,
        "end_station_id": pl.Utf8,
        "start_lat": pl.Float64,
        "start_lng": pl.Float64,
        "end_lat": pl.Float64,
        "end_lng": pl.Float64,
    }
    try:
        cast_expressions = []
        # Lấy schema một cách an toàn
        current_schema = lf.collect_schema()
        logger.info(f"Schema trước khi cast:\n{current_schema}")

        for col_name, target_type in dtype_mapping.items():
            if col_name in current_schema:
                 if current_schema[col_name] != target_type:
                      logger.info(f"Lên kế hoạch cast cột '{col_name}' từ {current_schema[col_name]} sang {target_type}")
                      cast_expressions.append(pl.col(col_name).cast(target_type))
                 else:
                      logger.info(f"Cột '{col_name}' đã có kiểu {target_type}, không cần cast.")
            else:
                 logger.warning(f"Không tìm thấy cột '{col_name}' trong schema để cast.")

        if cast_expressions:
             lf = lf.with_columns(cast_expressions)
             logger.info("Đã thêm bước chuyển đổi kiểu dữ liệu vào kế hoạch.")
             # Schema thực tế chỉ thay đổi khi collect
             # logger.info(f"Schema sau khi chuyển đổi (dự kiến):\n{lf.collect_schema()}")
        else:
             logger.info("Không có cột nào cần chuyển đổi kiểu dữ liệu.")

        return lf
    except Exception as e:
        logger.error(f"Lỗi khi lên kế hoạch chuyển đổi kiểu dữ liệu: {e}", exc_info=True)
        return lf

def calculate_rides_per_day(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Tính số lượng chuyến đi mỗi ngày."""
    logger.info("Lên kế hoạch tính số lượng chuyến đi mỗi ngày...")
    try:
        # Kiểm tra cột tồn tại trong schema (không cần collect)
        if "started_at" not in lf.collect_schema():
             logger.error("Thiếu cột 'started_at' để tính số chuyến đi mỗi ngày.")
             return pl.LazyFrame() # Trả về LazyFrame rỗng

        rides_per_day_lf = lf.group_by(
            pl.col("started_at").dt.date().alias("ride_date")
        ).agg(
            pl.count().alias("ride_count")
        ).sort("ride_date")
        return rides_per_day_lf
    except Exception as e:
        logger.error(f"Lỗi khi lên kế hoạch tính số chuyến đi mỗi ngày: {e}", exc_info=True)
        return pl.LazyFrame()

def calculate_weekly_ride_stats(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Tính số chuyến đi trung bình, max, min mỗi tuần."""
    logger.info("Lên kế hoạch tính số chuyến đi trung bình, max, min mỗi tuần...")
    try:
        if "started_at" not in lf.collect_schema():
             logger.error("Thiếu cột 'started_at' để tính thống kê tuần.")
             return pl.LazyFrame()

        rides_per_day_lf = lf.group_by(
            pl.col("started_at").dt.date().alias("ride_date")
        ).agg(
            pl.count().alias("ride_count")
        )
        rides_per_day_with_week_lf = rides_per_day_lf.with_columns(
            pl.col("ride_date").dt.week().alias("week_number")
        )
        weekly_stats_lf = rides_per_day_with_week_lf.group_by("week_number").agg([
            pl.mean("ride_count").alias("avg_rides_per_day"),
            pl.max("ride_count").alias("max_rides_per_day"),
            pl.min("ride_count").alias("min_rides_per_day")
        ]).sort("week_number")
        return weekly_stats_lf
    except Exception as e:
        logger.error(f"Lỗi khi lên kế hoạch tính thống kê theo tuần: {e}", exc_info=True)
        return pl.LazyFrame()

def calculate_daily_diff_vs_last_week(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Tính chênh lệch số chuyến đi so với cùng ngày tuần trước."""
    logger.info("Lên kế hoạch tính chênh lệch số chuyến đi so với cùng ngày tuần trước...")
    try:
        if "started_at" not in lf.collect_schema():
             logger.error("Thiếu cột 'started_at' để tính chênh lệch tuần.")
             return pl.LazyFrame()

        rides_per_day_lf = lf.group_by(
            pl.col("started_at").dt.date().alias("ride_date")
        ).agg(
            pl.count().alias("ride_count")
        ).sort("ride_date")

        rides_with_lag_lf = rides_per_day_lf.with_columns(
            pl.col("ride_count").shift(7).alias("rides_last_week")
        )
        daily_diff_lf = rides_with_lag_lf.with_columns(
            (pl.col("ride_count") - pl.col("rides_last_week")).alias("diff_vs_last_week")
        ).select(["ride_date", "ride_count", "rides_last_week", "diff_vs_last_week"])
        return daily_diff_lf
    except Exception as e:
        logger.error(f"Lỗi khi lên kế hoạch tính chênh lệch theo tuần: {e}", exc_info=True)
        return pl.LazyFrame()

# --- Luồng Thực thi Chính ---
if __name__ == "__main__":
    logger.info("--- Bắt đầu Exercise 9: Polars Lazy Computation ---")

    # 1. Đọc CSV vào LazyFrame
    lazy_df = read_csv_lazy(INPUT_CSV_FILE)

    # <<< SỬA LỖI KIỂM TRA Ở ĐÂY >>>
    # Kiểm tra xem read_csv_lazy có trả về None không (lỗi quét file)
    if lazy_df is not None:
        # Thực hiện các bước xử lý lazy
        # 2. Chuyển đổi kiểu dữ liệu
        lazy_df = convert_datatypes(lazy_df)

        # 3.1 Tính số chuyến đi mỗi ngày
        rides_per_day_lf = calculate_rides_per_day(lazy_df)

        # 3.2 Tính thống kê theo tuần
        weekly_stats_lf = calculate_weekly_ride_stats(lazy_df)

        # 3.3 Tính chênh lệch so với tuần trước
        daily_diff_lf = calculate_daily_diff_vs_last_week(lazy_df)

        # --- Thực thi và In kết quả ---
        # Chỉ khi gọi .collect() thì các phép tính mới thực sự diễn ra
        logger.info("\n--- Kết quả: Số chuyến đi mỗi ngày (Top 10) ---")
        try:
            # Thực thi kế hoạch rides_per_day_lf
            result_df = rides_per_day_lf.head(10).collect()
            print(result_df)
        except Exception as e:
            logger.error(f"Lỗi khi collect rides_per_day: {e}", exc_info=True)

        logger.info("\n--- Kết quả: Thống kê chuyến đi theo tuần (Top 10) ---")
        try:
            # Thực thi kế hoạch weekly_stats_lf
            result_df = weekly_stats_lf.head(10).collect()
            print(result_df)
        except Exception as e:
            logger.error(f"Lỗi khi collect weekly_stats: {e}", exc_info=True)

        logger.info("\n--- Kết quả: Chênh lệch số chuyến đi so với tuần trước (Top 10) ---")
        try:
            # Thực thi kế hoạch daily_diff_lf
            # Bỏ qua các dòng đầu không có dữ liệu tuần trước (NULL)
            result_df = daily_diff_lf.filter(pl.col("diff_vs_last_week").is_not_null()).head(10).collect()
            print(result_df)
        except Exception as e:
            logger.error(f"Lỗi khi collect daily_diff: {e}", exc_info=True)

    else:
        logger.error("Không thể đọc dữ liệu đầu vào. Dừng xử lý.")
        exit(1)

    logger.info("--- Kết thúc Exercise 9 ---")
