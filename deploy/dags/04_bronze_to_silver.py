from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'lakehouse_admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    '04_bronze_to_silver',
    default_args=default_args,
    description='Step 4: Clean and Enrich Bronze data to Silver layer',
    schedule_interval=None,
    catchup=False,
    tags=['lakehouse', 'silver', 'iceberg', 'step4']
) as dag:

    ICEBERG_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

    refine_silver = SparkSubmitOperator(
        task_id='refine_to_silver',
        application='/opt/spark/scripts/silver_cleansing.py',
        conn_id='spark_default',
        packages=ICEBERG_PACKAGES,
        name='bronze_to_iceberg_silver'
    )
