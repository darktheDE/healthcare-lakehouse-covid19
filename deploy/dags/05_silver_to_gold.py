from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'lakehouse_admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    '05_silver_to_gold',
    default_args=default_args,
    description='Step 5: Clinical Analytics from Silver to Gold layer',
    schedule_interval=None,
    catchup=False,
    tags=['lakehouse', 'gold', 'iceberg', 'step5']
) as dag:

    ICEBERG_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

    analyze_gold = SparkSubmitOperator(
        task_id='analyze_to_gold',
        application='/opt/spark/scripts/gold_clinical_analysis.py',
        conn_id='spark_default',
        packages=ICEBERG_PACKAGES,
        name='silver_to_iceberg_gold'
    )

    aggregate_gold = SparkSubmitOperator(
        task_id='aggregate_to_gold',
        application='/opt/spark/scripts/gold_aggregation.py',
        conn_id='spark_default',
        packages=ICEBERG_PACKAGES,
        name='silver_to_iceberg_gold_agg'
    )

    analyze_gold >> aggregate_gold
