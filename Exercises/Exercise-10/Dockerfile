# Bắt đầu từ một image Python ổn định
FROM python:3.9-slim

# Cài đặt git nếu GX cần clone thư viện hoặc ví dụ
# USER root
# RUN apt-get update && apt-get install -y --no-install-recommends git && apt-get clean && rm -rf /var/lib/apt/lists/*
# USER python

# Đặt thư mục làm việc
WORKDIR /app

# Sao chép file requirements
COPY requirements.txt .

# Cài đặt thư viện Python
# Nên tạo virtual environment để tránh xung đột, nhưng tạm thời cài global
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép mã nguồn
COPY main.py .
# Thư mục data sẽ được mount

# Lệnh mặc định
CMD ["python", "main.py"]
