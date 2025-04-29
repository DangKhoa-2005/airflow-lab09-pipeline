# Sử dụng image Airflow chính thức (Python 3.9)
# Chọn phiên bản ổn định, ví dụ 2.8.1
FROM apache/airflow:2.8.1-python3.9

# Cài đặt Docker CLI và Docker Compose bên trong container Airflow
# Cần thiết để Airflow có thể chạy lệnh docker và docker-compose từ BashOperator
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
       apt-transport-https \
       ca-certificates \
       curl \
       gnupg \
       lsb-release \
       docker.io \
       docker-compose && \
    # Dọn dẹp
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Thêm user airflow vào group docker để có quyền chạy lệnh docker
# Lưu ý: Việc này có thể có rủi ro bảo mật trong môi trường production thực tế
RUN usermod -aG docker airflow

# Chuyển về user airflow
USER airflow

# Đặt thư mục làm việc chuẩn của Airflow
WORKDIR /opt/airflow

# Sao chép file requirements.txt của môi trường Airflow
COPY requirements.txt .

# Cài đặt các thư viện Python cần thiết cho Airflow
# Cài đặt vào môi trường của user airflow
RUN pip install --user --no-cache-dir -r requirements.txt

# Không cần copy src hay data của các bài tập vào image này
# Chúng sẽ được mount thông qua volume trong docker-compose.yml
