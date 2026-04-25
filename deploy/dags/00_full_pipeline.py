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
    '00_full_pipeline',
    default_args=default_args,
    description='Full Pipeline: Steps 01 to 05 (CSV -> Postgres -> Raw -> Bronze -> Silver -> Gold)',
    schedule_interval=None,
    catchup=False,
    tags=['lakehouse', 'full_pipeline', 'iceberg']
) as dag:

    # Configuration Constants
    POSTGRES_JAR = '/opt/spark/jars/postgresql-42.7.3.jar'
    S3_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ICEBERG_PACKAGES = f"org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,{S3_PACKAGES}"

    # --- STEP 1: CSV to PostgreSQL ---
    ingest_patients = SparkSubmitOperator(
        task_id='step1_ingest_patients',
        application='/opt/spark/scripts/ingest_patients.py',
        conn_id='spark_default',
        jars=POSTGRES_JAR,
        driver_class_path=POSTGRES_JAR,
        name='full_pipeline_ingest_patients'
    )

    ingest_encounters = SparkSubmitOperator(
        task_id='step1_ingest_encounters',
        application='/opt/spark/scripts/ingest_encounters.py',
        conn_id='spark_default',
        jars=POSTGRES_JAR,
        driver_class_path=POSTGRES_JAR,
        name='full_pipeline_ingest_encounters'
    )

    ingest_conditions = SparkSubmitOperator(
        task_id='step1_ingest_conditions',
        application='/opt/spark/scripts/ingest_conditions.py',
        conn_id='spark_default',
        jars=POSTGRES_JAR,
        driver_class_path=POSTGRES_JAR,
        name='full_pipeline_ingest_conditions'
    )

    # --- STEP 2: PostgreSQL to Raw (Parquet) ---
    export_to_raw = SparkSubmitOperator(
        task_id='step2_export_to_raw',
        application='/opt/spark/scripts/postgres_to_raw.py',
        conn_id='spark_default',
        packages=S3_PACKAGES,
        jars=POSTGRES_JAR,
        driver_class_path=POSTGRES_JAR,
        name='full_pipeline_postgres_to_raw'
    )

    # --- STEP 3: Raw to Bronze (Iceberg) ---
    ingest_bronze = SparkSubmitOperator(
        task_id='step3_ingest_to_bronze',
        application='/opt/spark/scripts/bronze_ingestion.py',
        conn_id='spark_default',
        packages=ICEBERG_PACKAGES,
        name='full_pipeline_raw_to_bronze'
    )

    # --- STEP 4: Bronze to Silver (Cleaning) ---
    refine_silver = SparkSubmitOperator(
        task_id='step4_refine_to_silver',
        application='/opt/spark/scripts/silver_cleansing.py',
        conn_id='spark_default',
        packages=ICEBERG_PACKAGES,
        name='full_pipeline_bronze_to_silver'
    )

    # --- STEP 5: Silver to Gold (Analytics) ---
    analyze_gold = SparkSubmitOperator(
        task_id='step5_analyze_to_gold',
        application='/opt/spark/scripts/gold_clinical_analysis.py',
        conn_id='spark_default',
        packages=ICEBERG_PACKAGES,
        name='full_pipeline_gold_analysis'
    )

    aggregate_gold = SparkSubmitOperator(
        task_id='step5_aggregate_to_gold',
        application='/opt/spark/scripts/gold_aggregation.py',
        conn_id='spark_default',
        packages=ICEBERG_PACKAGES,
        name='full_pipeline_gold_aggregation'
    )

    # Chain of full pipeline dependencies
    # Step 1 Sequential
    ingest_patients >> ingest_encounters >> ingest_conditions 
    # Step 1 -> Step 2
    ingest_conditions >> export_to_raw
    # Step 2 -> Step 3
    export_to_raw >> ingest_bronze
    # Step 3 -> Step 4
    ingest_bronze >> refine_silver
    # Step 4 -> Step 5
    refine_silver >> analyze_gold
    refine_silver >> aggregate_gold
