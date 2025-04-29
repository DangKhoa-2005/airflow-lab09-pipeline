# Bắt đầu từ một image Python ổn định
FROM python:3.9-slim

# Có thể cần cài đặt build-essential nếu duckdb cần biên dịch C extensions
# USER root
# RUN apt-get update && apt-get install -y --no-install-recommends build-essential && apt-get clean && rm -rf /var/lib/apt/lists/*
# USER python

# Đặt thư mục làm việc
WORKDIR /app

# Sao chép file requirements
COPY requirements.txt .

# Cài đặt thư viện Python
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép mã nguồn
COPY main.py .
# Thư mục data sẽ được mount

# Lệnh mặc định
CMD ["python", "main.py"]
