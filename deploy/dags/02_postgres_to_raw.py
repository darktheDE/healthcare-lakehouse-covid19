from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'lakehouse_admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    '02_postgres_to_raw',
    default_args=default_args,
    description='Step 2: Export data from PostgreSQL to MinIO Raw (Parquet)',
    schedule_interval=None,
    catchup=False,
    tags=['lakehouse', 'ingestion', 'step2']
) as dag:

    # Cần thêm package hadoop-aws để ghi lên S3/MinIO
    # Và Jar Postgres để đọc
    S3_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    POSTGRES_JAR = '/opt/spark/jars/postgresql-42.7.3.jar'

    export_to_raw = SparkSubmitOperator(
        task_id='export_to_raw',
        application='/opt/spark/scripts/postgres_to_raw.py',
        conn_id='spark_default',
        packages=S3_PACKAGES,
        jars=POSTGRES_JAR,
        driver_class_path=POSTGRES_JAR,
        name='postgres_to_raw_parquet'
    )
