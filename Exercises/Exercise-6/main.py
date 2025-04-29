import logging
from pathlib import Path
from pyspark.sql import SparkSession
# <<< THÊM IMPORT DataFrame >>>
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.window import Window
from typing import Optional, Tuple
from datetime import timedelta
# Không cần import pandas nữa nếu chỉ dùng cho type hint sai
# import pandas as pd

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
INPUT_FILES_PATTERN = str(DATA_DIR / "*.zip")

# --- Khởi tạo SparkSession ---
def create_spark_session(app_name="Exercise6"):
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

# --- Định nghĩa Schema ---
schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("bikeid", IntegerType(), True),
    StructField("tripduration", DoubleType(), True),
    StructField("from_station_id", IntegerType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("to_station_id", IntegerType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthyear", IntegerType(), True),
])

# --- Các Hàm Xử lý ---

# <<< SỬA TYPE HINT Ở ĐÂY >>>
def read_data(spark: SparkSession, path_pattern: str, schema: StructType) -> Optional[DataFrame]:
    """Đọc dữ liệu từ các file CSV (nén hoặc không) vào Spark DataFrame."""
    logger.info(f"Đang đọc dữ liệu từ: {path_pattern}")
    try:
        df = spark.read.csv(
            path_pattern,
            header=True,
            schema=schema,
            timestampFormat="yyyy-MM-dd HH:mm:ss"
        )
        logger.info(f"Đọc dữ liệu thành công. Số lượng bản ghi: {df.count()}")
        return df
    except Exception as e:
        logger.error(f"Lỗi khi đọc dữ liệu từ {path_pattern}: {e}", exc_info=True)
        return None

# <<< SỬA TYPE HINT Ở ĐÂY >>>
def calculate_average_duration_per_day(df: DataFrame) -> Optional[DataFrame]:
    """Câu 1: Tính thời gian chuyến đi trung bình mỗi ngày."""
    logger.info("Câu 1: Tính thời gian chuyến đi trung bình mỗi ngày...")
    try:
        df_with_date = df.withColumn("start_date", F.to_date(F.col("start_time")))
        avg_duration_df = df_with_date.groupBy("start_date") \
            .agg(F.avg("tripduration").alias("average_duration_seconds")) \
            .orderBy("start_date")
        return avg_duration_df
    except Exception as e:
        logger.error(f"Lỗi khi tính câu 1: {e}", exc_info=True)
        return None

# <<< SỬA TYPE HINT Ở ĐÂY >>>
def count_trips_per_day(df: DataFrame) -> Optional[DataFrame]:
    """Câu 2: Đếm số chuyến đi mỗi ngày."""
    logger.info("Câu 2: Đếm số chuyến đi mỗi ngày...")
    try:
        df_with_date = df.withColumn("start_date", F.to_date(F.col("start_time")))
        trip_counts_df = df_with_date.groupBy("start_date") \
            .count() \
            .withColumnRenamed("count", "total_trips") \
            .orderBy("start_date")
        return trip_counts_df
    except Exception as e:
        logger.error(f"Lỗi khi tính câu 2: {e}", exc_info=True)
        return None

# <<< SỬA TYPE HINT Ở ĐÂY >>>
def most_popular_start_station_per_month(df: DataFrame) -> Optional[DataFrame]:
    """Câu 3: Tìm trạm xuất phát phổ biến nhất mỗi tháng."""
    logger.info("Câu 3: Tìm trạm xuất phát phổ biến nhất mỗi tháng...")
    try:
        df_with_month = df.withColumn("start_month", F.date_format(F.col("start_time"), "yyyy-MM"))
        station_counts = df_with_month.groupBy("start_month", "from_station_id", "from_station_name") \
            .count()
        window_spec = Window.partitionBy("start_month").orderBy(F.col("count").desc())
        ranked_stations = station_counts.withColumn("rank", F.rank().over(window_spec))
        most_popular_df = ranked_stations.filter(F.col("rank") == 1) \
            .select("start_month", "from_station_id", "from_station_name", F.col("count").alias("trip_count")) \
            .orderBy("start_month")
        return most_popular_df
    except Exception as e:
        logger.error(f"Lỗi khi tính câu 3: {e}", exc_info=True)
        return None

# <<< SỬA TYPE HINT Ở ĐÂY >>>
def top_3_stations_last_two_weeks(df: DataFrame) -> Optional[DataFrame]:
    """Câu 4: Top 3 trạm (xuất phát) phổ biến nhất mỗi ngày trong 2 tuần cuối cùng."""
    logger.info("Câu 4: Top 3 trạm phổ biến nhất mỗi ngày trong 2 tuần cuối cùng...")
    try:
        max_date_result = df.agg(F.max("start_time")).first()
        if not max_date_result or max_date_result[0] is None:
            logger.warning("Không tìm thấy ngày cuối cùng trong dữ liệu.")
            return None
        max_date = max_date_result[0]

        two_weeks_ago = max_date - timedelta(days=14)
        logger.info(f"Ngày cuối cùng: {max_date}, Ngày bắt đầu 2 tuần cuối: {two_weeks_ago}")

        df_last_two_weeks = df.filter(F.col("start_time") >= two_weeks_ago) \
                              .withColumn("start_date", F.to_date(F.col("start_time")))

        station_counts_daily = df_last_two_weeks.groupBy("start_date", "from_station_id", "from_station_name") \
            .count()

        window_spec = Window.partitionBy("start_date").orderBy(F.col("count").desc())
        ranked_stations_daily = station_counts_daily.withColumn("rank", F.rank().over(window_spec))
        top_3_df = ranked_stations_daily.filter(F.col("rank") <= 3) \
            .select("start_date", "rank", "from_station_id", "from_station_name", F.col("count").alias("trip_count")) \
            .orderBy("start_date", "rank")
        return top_3_df
    except Exception as e:
        logger.error(f"Lỗi khi tính câu 4: {e}", exc_info=True)
        return None

# <<< SỬA TYPE HINT Ở ĐÂY >>>
def average_duration_by_gender(df: DataFrame) -> Optional[DataFrame]:
    """Câu 5: So sánh thời gian chuyến đi trung bình giữa Nam và Nữ."""
    logger.info("Câu 5: So sánh thời gian chuyến đi trung bình giữa Nam và Nữ...")
    try:
        gender_duration_df = df.filter(F.col("gender").isin(["Male", "Female"])) \
            .groupBy("gender") \
            .agg(F.avg("tripduration").alias("average_duration_seconds"))
        return gender_duration_df
    except Exception as e:
        logger.error(f"Lỗi khi tính câu 5: {e}", exc_info=True)
        return None

# <<< SỬA TYPE HINT Ở ĐÂY >>>
def top_10_ages_longest_shortest_trips(df: DataFrame) -> Tuple[Optional[DataFrame], Optional[DataFrame]]:
    """Câu 6: Top 10 độ tuổi có chuyến đi dài nhất và ngắn nhất."""
    logger.info("Câu 6: Top 10 độ tuổi có chuyến đi dài nhất và ngắn nhất...")
    try:
        current_year_result = df.agg(F.max(F.year("start_time"))).first()
        if not current_year_result or current_year_result[0] is None:
            logger.warning("Không xác định được năm để tính tuổi.")
            return None, None
        current_year = current_year_result[0]
        logger.info(f"Năm hiện tại được xác định để tính tuổi: {current_year}")

        df_with_age = df.filter(F.col("birthyear").isNotNull()) \
                        .withColumn("age", F.lit(current_year) - F.col("birthyear"))

        df_valid_age = df_with_age.filter((F.col("age") >= 10) & (F.col("age") <= 100))
        logger.info(f"Số bản ghi sau khi lọc tuổi hợp lệ: {df_valid_age.count()}")

        if df_valid_age.rdd.isEmpty():
             logger.warning("Không còn dữ liệu sau khi lọc tuổi hợp lệ.")
             return None, None

        avg_duration_by_age = df_valid_age.groupBy("age") \
            .agg(F.avg("tripduration").alias("average_duration"))

        longest_trips_df = avg_duration_by_age.orderBy(F.col("average_duration").desc()).limit(10)
        shortest_trips_df = avg_duration_by_age.orderBy(F.col("average_duration").asc()).limit(10)

        logger.info("Top 10 tuổi có chuyến đi dài nhất:")
        longest_trips_df.show(truncate=False)
        logger.info("Top 10 tuổi có chuyến đi ngắn nhất:")
        shortest_trips_df.show(truncate=False)

        return longest_trips_df, shortest_trips_df
    except Exception as e:
        logger.error(f"Lỗi khi tính câu 6: {e}", exc_info=True)
        return None, None

# <<< SỬA TYPE HINT Ở ĐÂY >>>
def save_report(df: Optional[DataFrame], report_name: str):
    """Lưu Spark DataFrame vào file CSV trong thư mục reports."""
    if df is None:
        logger.warning(f"Không có dữ liệu để lưu cho báo cáo: {report_name}")
        return
    if not df.head(1):
         logger.warning(f"DataFrame cho báo cáo '{report_name}' rỗng, không ghi file.")
         return

    output_path = REPORTS_DIR / f"{report_name}"
    logger.info(f"Đang lưu báo cáo '{report_name}' vào thư mục: {output_path}")
    try:
        df.coalesce(1).write.csv(str(output_path), header=True, mode="overwrite")
        logger.info(f"Lưu báo cáo '{report_name}' thành công vào thư mục {output_path}.")
    except Exception as e:
        logger.error(f"Lỗi khi lưu báo cáo '{report_name}': {e}", exc_info=True)

# --- Luồng Thực thi Chính ---
if __name__ == "__main__":
    logger.info("--- Bắt đầu Exercise 6: PySpark Aggregation ---")

    spark = None
    try:
        spark = create_spark_session()
        trips_df = read_data(spark, INPUT_FILES_PATTERN, schema)

        if trips_df:
            trips_df.cache()
            logger.info("DataFrame đã được cache.")

            REPORTS_DIR.mkdir(parents=True, exist_ok=True)

            report1 = calculate_average_duration_per_day(trips_df)
            save_report(report1, "average_duration_per_day")

            report2 = count_trips_per_day(trips_df)
            save_report(report2, "trips_per_day")

            report3 = most_popular_start_station_per_month(trips_df)
            save_report(report3, "most_popular_start_station_per_month")

            report4 = top_3_stations_last_two_weeks(trips_df)
            save_report(report4, "top_3_stations_last_two_weeks")

            report5 = average_duration_by_gender(trips_df)
            save_report(report5, "average_duration_by_gender")

            report6_longest, report6_shortest = top_10_ages_longest_shortest_trips(trips_df)
            save_report(report6_longest, "top_10_ages_longest_trips")
            save_report(report6_shortest, "top_10_ages_shortest_trips")

            trips_df.unpersist()
            logger.info("DataFrame đã được unpersist.")

        else:
            logger.error("Không thể đọc dữ liệu đầu vào. Dừng xử lý.")
            exit(1)

    except Exception as e:
         logger.error(f"Lỗi không mong muốn trong quá trình xử lý chính: {e}", exc_info=True)
         exit(1)
    finally:
        if spark:
            logger.info("Đang dừng SparkSession...")
            spark.stop()
            logger.info("SparkSession đã dừng.")

    logger.info("--- Kết thúc Exercise 6 ---")
