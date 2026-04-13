from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'lakehouse_admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    '03_raw_to_bronze',
    default_args=default_args,
    description='Step 3: Ingest Raw Parquet into Iceberg Bronze tables',
    schedule_interval=None,
    catchup=False,
    tags=['lakehouse', 'bronze', 'iceberg', 'step3']
) as dag:

    # Iceberg + S3 Packages
    ICEBERG_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

    ingest_bronze = SparkSubmitOperator(
        task_id='ingest_to_bronze',
        application='/opt/spark/scripts/bronze_ingestion.py',
        conn_id='spark_default',
        packages=ICEBERG_PACKAGES,
        name='raw_to_iceberg_bronze'
    )
