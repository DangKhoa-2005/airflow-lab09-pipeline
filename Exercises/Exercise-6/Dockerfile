# Bắt đầu từ một image Python ổn định
FROM python:3.9-slim

# Cài đặt Java (OpenJDK) - Spark cần Java Runtime Environment (JRE)
# Chạy các lệnh cài đặt hệ thống với quyền root (mặc định)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# USER python # <<< XÓA DÒNG NÀY

# Đặt thư mục làm việc
WORKDIR /app

# Sao chép file requirements
COPY requirements.txt .

# Cài đặt thư viện Python (bao gồm pyspark) - Chạy với quyền root
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép mã nguồn và dữ liệu
COPY main.py .
# Thư mục data sẽ được mount qua docker-compose, không cần COPY ở đây

# Tạo thư mục reports để lưu kết quả
RUN mkdir reports

# Lệnh mặc định (sẽ bị ghi đè bởi docker-compose)
CMD ["python", "main.py"]
