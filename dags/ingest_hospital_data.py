from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'lakehouse_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ingest_hospital_data_from_postgres',
    default_args=default_args,
    description='DAG trích xuất dữ liệu từ Postgres lên MinIO Raw bằng Spark',
    schedule_interval=None,  # Chạy tay hoặc trigger
    catchup=False,
    tags=['ingestion', 'raw', 'postgres'],
) as dag:

    # Task này sẽ chạy container spark-raw-job đã cấu hình trong docker-compose
    # Lưu ý: Do ta đã có service trong docker-compose, ta có thể dùng BashOperator 
    # để gọi lệnh docker-compose start, hoặc dùng DockerOperator.
    # Ở đây dùng giải pháp đơn giản nhất cho LAB là trigger service có sẵn.
    
    from airflow.operators.bash import BashOperator

    ingest_task = BashOperator(
        task_id='run_spark_ingestion_job',
        bash_command='docker start -a spark-raw-job',
    )

    ingest_task
