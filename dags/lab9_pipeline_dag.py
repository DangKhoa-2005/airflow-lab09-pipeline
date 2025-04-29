from __future__ import annotations

import pendulum
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from typing import Final

# --- Hằng số ---
PROJECT_BASE_PATH: Final[str] = "/opt/airflow/project"
EXERCISES_BASE_PATH: Final[str] = f"{PROJECT_BASE_PATH}/Exercises"
PYTHON_EXE: Final[str] = "python3" # Giữ nguyên python3

# --- Default Arguments cho DAG ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_alert_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, # Lưu ý: Không có retry tự động nếu task fail
}

# --- Định nghĩa DAG ---
with DAG(
    dag_id='lab09_exercises_1_to_5_pipeline',
    default_args=default_args,
    description='Pipeline tự động chạy Exercise 1-5 của LAB 09',
    schedule_interval=None, # Chỉ chạy thủ công
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=['lab09', 'data-engineering-practice', 'pipeline', 'exercises'],
) as dag:

    start = EmptyOperator(task_id='start_pipeline')

    # --- Task cho từng Exercise ---

    run_exercise_1 = BashOperator(
        task_id='run_exercise_1',
        bash_command=f"""
            echo "--- Running Exercise 1 ---" && \
            cd {EXERCISES_BASE_PATH}/Exercise-1 && \
            echo "Current directory: $(pwd)" && \
            echo "Building image exercise-1..." && \
            docker build --tag=exercise-1 . && \
            echo "Running docker-compose run for Exercise 1..." && \
            # echo "Listing contents of /app inside the container:" && \ # Gợi ý: Có thể xóa dòng ls này vì đã debug xong lỗi file not found
            # docker-compose run --rm run ls -la /app && \ # Gợi ý: Có thể xóa dòng ls này
            echo "Attempting to run python script..." && \
            # QUAN TRỌNG: Đảm bảo docker-compose.yml của Exercise-1 KHÔNG CÓ 'volumes: - .:/app' cho service 'run'
            docker-compose run --rm run {PYTHON_EXE} /app/main.py && \
            echo "Exercise 1 finished successfully. Cleaning up..." && \
            docker-compose down && \
            echo "--- Exercise 1 Cleanup Done ---"
        """,
        # Timeout 30 phút cho Exercise 1 là hợp lý vì nó mất ~10 phút
        execution_timeout=timedelta(minutes=30),
    )

    run_exercise_2 = BashOperator(
        task_id='run_exercise_2',
        bash_command=f"""
            echo "--- Running Exercise 2 ---" && \
            cd {EXERCISES_BASE_PATH}/Exercise-2 && \
            echo "Current directory: $(pwd)" && \
            echo "Building image exercise-2..." && \
            docker build --tag=exercise-2 . && \
            echo "Running docker-compose run for Exercise 2..." && \
            # QUAN TRỌNG: Đảm bảo docker-compose.yml của Exercise-2 KHÔNG CÓ 'volumes: - .:/app' cho service 'run'
            docker-compose run --rm run {PYTHON_EXE} /app/main.py && \
            echo "Exercise 2 finished successfully. Cleaning up..." && \
            docker-compose down && \
            echo "--- Exercise 2 Cleanup Done ---"
        """,
        # Timeout 10 phút. Cần theo dõi xem có đủ không khi chạy thực tế.
        execution_timeout=timedelta(minutes=10),
    )

    run_exercise_3 = BashOperator(
        task_id='run_exercise_3',
        bash_command=f"""
            echo "--- Running Exercise 3 ---" && \
            cd {EXERCISES_BASE_PATH}/Exercise-3 && \
            echo "Current directory: $(pwd)" && \
            echo "Building image exercise-3..." && \
            docker build --tag=exercise-3 . && \
            echo "Running docker-compose run for Exercise 3..." && \
            # QUAN TRỌNG: Đảm bảo docker-compose.yml của Exercise-3 KHÔNG CÓ 'volumes: - .:/app' cho service 'run'
            docker-compose run --rm run {PYTHON_EXE} /app/main.py && \
            echo "Exercise 3 finished successfully. Cleaning up..." && \
            docker-compose down && \
            echo "--- Exercise 3 Cleanup Done ---"
        """,
         # Timeout 15 phút. Cần theo dõi xem có đủ không khi chạy thực tế.
        execution_timeout=timedelta(minutes=15),
    )

    run_exercise_4 = BashOperator(
        task_id='run_exercise_4',
        bash_command=f"""
            echo "--- Running Exercise 4 ---" && \
            cd {EXERCISES_BASE_PATH}/Exercise-4 && \
            echo "Current directory: $(pwd)" && \
            echo "Building image exercise-4..." && \
            docker build --tag=exercise-4 . && \
            echo "Running docker-compose run for Exercise 4..." && \
            # QUAN TRỌNG: Đảm bảo docker-compose.yml của Exercise-4 KHÔNG CÓ 'volumes: - .:/app' cho service 'run'
            docker-compose run --rm run {PYTHON_EXE} /app/main.py && \
            echo "Exercise 4 finished successfully. Cleaning up..." && \
            docker-compose down && \
            echo "--- Exercise 4 Cleanup Done ---"
        """,
        # Timeout 5 phút. Có thể hơi ngắn, cần theo dõi xem có đủ không.
        execution_timeout=timedelta(minutes=5),
    )

    run_exercise_5 = BashOperator(
        task_id='run_exercise_5',
        bash_command=f"""
            echo "--- Running Exercise 5 ---" && \
            cd {EXERCISES_BASE_PATH}/Exercise-5 && \
            echo "Current directory: $(pwd)" && \
            echo "Building image exercise-5..." && \
            docker build --tag=exercise-5 . && \
            echo "Running docker-compose run for Exercise 5..." && \
            # QUAN TRỌNG: Đảm bảo docker-compose.yml của Exercise-5 KHÔNG CÓ 'volumes: - .:/app' cho service 'run'
            docker-compose run --rm run {PYTHON_EXE} /app/main.py && \
            echo "Exercise 5 finished successfully. Cleaning up..." && \
            # Lưu ý: Dùng -v ở đây để xóa cả volume, khác với các task khác. Có chủ ý không?
            docker-compose down -v && \
            echo "--- Exercise 5 Cleanup Done (with volume) ---"
        """,
         # Timeout 10 phút. Cần theo dõi xem có đủ không khi chạy thực tế.
        execution_timeout=timedelta(minutes=10),
    )

    end = EmptyOperator(
        task_id='end_pipeline',
        trigger_rule=TriggerRule.ALL_SUCCESS, # Chỉ chạy nếu tất cả task trước đó thành công
    )

    # Định nghĩa thứ tự chạy tuần tự
    start >> run_exercise_1 >> run_exercise_2 >> run_exercise_3 >> run_exercise_4 >> run_exercise_5 >> end