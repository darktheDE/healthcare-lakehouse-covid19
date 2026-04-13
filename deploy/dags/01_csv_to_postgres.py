from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'lakehouse_admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '01_csv_to_postgres',
    default_args=default_args,
    description='Step 1: Ingest raw CSV data from source to PostgreSQL',
    schedule_interval=None,
    catchup=False,
    tags=['lakehouse', 'ingestion', 'step1']
) as dag:

    # File JAR Postgres cần thiết
    POSTGRES_JAR = '/opt/spark/jars/postgresql-42.7.3.jar'

    ingest_patients = SparkSubmitOperator(
        task_id='ingest_patients',
        application='/opt/spark/scripts/ingest_patients.py',
        conn_id='spark_default',
        jars=POSTGRES_JAR,
        driver_class_path=POSTGRES_JAR,
        name='ingest_patients'
    )

    ingest_encounters = SparkSubmitOperator(
        task_id='ingest_encounters',
        application='/opt/spark/scripts/ingest_encounters.py',
        conn_id='spark_default',
        jars=POSTGRES_JAR,
        driver_class_path=POSTGRES_JAR,
        name='ingest_encounters'
    )

    ingest_conditions = SparkSubmitOperator(
        task_id='ingest_conditions',
        application='/opt/spark/scripts/ingest_conditions.py',
        conn_id='spark_default',
        jars=POSTGRES_JAR,
        driver_class_path=POSTGRES_JAR,
        name='ingest_conditions'
    )

    # Chạy lần lượt (vì có Foreign Key constraints: patients -> encounters -> conditions)
    ingest_patients >> ingest_encounters >> ingest_conditions
