# Sử dụng một base image Python ổn định (ví dụ: 3.9-slim)
FROM python:3.9-slim

# Đặt thư mục làm việc bên trong image
WORKDIR /app

# Sao chép file requirements trước để tận dụng cache Docker
# Đảm bảo file requirements.txt tồn tại trong Exercises/Exercise-1
COPY requirements.txt .

# Cài đặt các thư viện cần thiết
# Sử dụng python3 hoặc python tùy thuộc vào base image
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# *** LỆNH QUAN TRỌNG NHẤT: Sao chép TOÀN BỘ nội dung thư mục hiện tại ***
# *** (bao gồm main.py và các file khác nếu có) vào thư mục /app trong image ***
COPY . .

# Lệnh mặc định để chạy script (sẽ được ghi đè bởi docker-compose run trong DAG)
# CMD ["python3", "main.py"]
