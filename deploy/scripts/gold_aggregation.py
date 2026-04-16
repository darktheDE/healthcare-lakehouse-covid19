"""
gold_aggregation.py
-------------------
Build a Gold analytical table for Symptoms and Outcomes analysis.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession, functions as F


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv(
    "AWS_ACCESS_KEY_ID",
    os.getenv("MINIO_LAKEHOUSE_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "admin")),
)
MINIO_SECRET_KEY = os.getenv(
    "AWS_SECRET_ACCESS_KEY",
    os.getenv("MINIO_LAKEHOUSE_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "password")),
)
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "hospital-lakehouse")

CATALOG_NAME = "hospital"
SILVER_NAMESPACE = f"{CATALOG_NAME}.silver"
GOLD_NAMESPACE = f"{CATALOG_NAME}.gold"

COVID_MASTER_TABLE = f"{SILVER_NAMESPACE}.covid_clinical_master"
CONDITIONS_TABLE = f"{SILVER_NAMESPACE}.conditions"
OUTPUT_TABLE = f"{GOLD_NAMESPACE}.symptoms_outcomes"
MORTALITY_TABLE = f"{GOLD_NAMESPACE}.mortality_demographics"
ENCOUNTERS_TABLE = f"{SILVER_NAMESPACE}.encounters"
HOSPITALIZATION_TABLE = f"{GOLD_NAMESPACE}.hospitalization_workload"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("gold-aggregation")
        .config("spark.sql.catalog.hospital", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hospital.type", "hive")
        .config("spark.sql.catalog.hospital.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.hospital.warehouse", f"s3a://{MINIO_BUCKET}/")
        .config("spark.sql.catalog.hospital.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.hospital.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.hospital.s3.access-key-id", MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.hospital.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.catalog.hospital.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.defaultCatalog", CATALOG_NAME)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def wait_for_table(
    spark: SparkSession,
    table_name: str,
    timeout_seconds: int = 300,
    required: bool = True,
) -> DataFrame | None:
    try:
        return spark.table(table_name)
    except Exception as exc:
        if not required:
            return None
        raise RuntimeError(f"Table {table_name} was not ready") from exc


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 72)
    print("  Silver -> Gold COVID Symptoms & Outcomes aggregation job")
    print("=" * 72)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}")

    print("[INFO] Waiting for Silver tables to be ready...")
    covid_master_df = wait_for_table(spark, COVID_MASTER_TABLE)
    conditions_df = wait_for_table(spark, CONDITIONS_TABLE)
    
    # 1. Labeling Survivor/Non-survivor and extracting population limits
    cohort_df = covid_master_df.select(
        "patient_id",
        F.when(F.col("patient_deathdate").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_survivor")
    ).cache()

    total_survivors = cohort_df.filter(F.col("is_survivor") == True).count()
    total_non_survivors = cohort_df.filter(F.col("is_survivor") == False).count()
    
    print(f"[INFO] Cohort stats - Survivors: {total_survivors}, Non-survivors: {total_non_survivors}")

    # 2. Get COVID-19 diagnosis date per patient
    # Condition code '840539006' represents COVID-19
    covid_diagnosis_df = (
        conditions_df.filter(F.col("condition_code") == "840539006")
        .groupBy("patient_id")
        .agg(F.min("condition_start_date").alias("covid_diagnosis_date"))
    )

    # 3. Filter target conditions and join
    TARGET_KEYWORDS = "Sepsis|ARDS|Acute respiratory distress syndrome|Heart failure|Cough|Fever"
    
    filtered_conditions = conditions_df.filter(
        F.col("condition_description").rlike(f"(?i)({TARGET_KEYWORDS})")
    )

    # Combine: cohort -> diagnosis date -> target conditions
    analysis_df = (
        cohort_df
        .join(covid_diagnosis_df, on="patient_id", how="inner")
        .join(filtered_conditions, on="patient_id", how="inner")
    )

    # Only keep conditions that started ON OR AFTER the COVID-19 diagnosis date
    post_covid_conditions = analysis_df.filter(
        F.col("condition_start_date") >= F.col("covid_diagnosis_date")
    ).select(
        "patient_id", 
        "is_survivor", 
        # Standardize labels
        F.when(F.col("condition_description").rlike("(?i)(Sepsis)"), "Sepsis")
        .when(F.col("condition_description").rlike("(?i)(ARDS|Acute respiratory distress s)"), "ARDS")
        .when(F.col("condition_description").rlike("(?i)(Heart failure)"), "Heart Failure")
        .when(F.col("condition_description").rlike("(?i)(Cough)"), "Cough")
        .when(F.col("condition_description").rlike("(?i)(Fever)"), "Fever")
        .otherwise(F.col("condition_description")).alias("condition_category")
    )

    # 4. Aggregation
    agg_df = (
        post_covid_conditions
        .groupBy("condition_category", "is_survivor")
        .agg(F.countDistinct("patient_id").alias("patient_count"))
    )

    # 5. Calculation Logic (% Rate)
    # Add the total denominator back based on the is_survivor flag
    final_df = agg_df.withColumn(
        "total_group_population",
        F.when(F.col("is_survivor") == True, F.lit(total_survivors))
         .otherwise(F.lit(total_non_survivors))
    ).withColumn(
        "rate_percentage",
        F.round((F.col("patient_count") / F.col("total_group_population")) * 100, 2)
    ).select(
        "condition_category",
        "is_survivor",
        "patient_count",
        "total_group_population",
        "rate_percentage"
    ).orderBy("condition_category", "is_survivor")

    result_count = final_df.count()
    print(f"[INFO] Symptoms Outcomes logic completed. Result rows: {result_count}")
    final_df.show(truncate=False)

    print(f"[INFO] Writing target table: {OUTPUT_TABLE}")
    final_df.writeTo(OUTPUT_TABLE).createOrReplace()

    print("\n[CHECK] Hive Metastore metadata for target table:")
    print(f"  - table exists: {spark.catalog.tableExists(OUTPUT_TABLE)}")

    # --------------------------------------------------------------------------------
    # TASK 2.2: Mortality Demographics (Phân tích Tỷ lệ tử vong theo độ tuổi và giới tính)
    # --------------------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("  Mortality Demographics logic starting...")
    print("=" * 72)

    # 1. Lọc bệnh nhân tử vong
    mortality_cohort = covid_master_df.filter(F.col("patient_deathdate").isNotNull())

    # 2. Tính tuổi lúc mất
    mortality_with_age = mortality_cohort.withColumn(
        "age_at_death",
        F.year("patient_deathdate") - F.year("patient_birthdate")
    )

    # 3. Phân nhóm tuổi (Age Binning)
    mortality_binned = mortality_with_age.withColumn(
        "Age_Range",
        F.when(F.col("patient_birthdate").isNull(), "Unknown")
         .when(F.col("age_at_death") <= 10, "0-10")
         .when(F.col("age_at_death") <= 20, "11-20")
         .when(F.col("age_at_death") <= 30, "21-30")
         .when(F.col("age_at_death") <= 40, "31-40")
         .when(F.col("age_at_death") <= 50, "41-50")
         .when(F.col("age_at_death") <= 60, "51-60")
         .when(F.col("age_at_death") <= 70, "61-70")
         .when(F.col("age_at_death") <= 80, "71-80")
         .otherwise("80+")
    )

    # 4. Aggregation
    mortality_agg = (
        mortality_binned
        .groupBy("Age_Range", "gender")
        .agg(F.count("patient_id").alias("mortality_count"))
        .orderBy("Age_Range", "gender")
    )

    mortality_result_count = mortality_agg.count()
    print(f"[INFO] Mortality logic completed. Result rows: {mortality_result_count}")
    mortality_agg.show(truncate=False)

    print(f"[INFO] Writing target table: {MORTALITY_TABLE}")
    mortality_agg.writeTo(MORTALITY_TABLE).createOrReplace()

    print("\n[CHECK] Hive Metastore metadata for target table:")
    print(f"  - table exists: {spark.catalog.tableExists(MORTALITY_TABLE)}")

    # --------------------------------------------------------------------------------
    # TASK 2.3: Hospitalization & ICU Workload (Phân tích gánh nặng y tế)
    # --------------------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("  Hospitalization & ICU Workload logic starting...")
    print("=" * 72)

    print("[INFO] Waiting for silver encounters table to be ready...")
    encounters_df = wait_for_table(spark, ENCOUNTERS_TABLE)

    total_covid_patients = covid_master_df.count()

    # Lọc encounters code (1505002 - Hospital Admission, 305351004 - ICU Admission)
    target_encounters = encounters_df.filter(
        F.col("encounter_code").isin("1505002", "305351004")
    )

    # Chỉ lấy bệnh nhân dính COVID-19
    covid_encounters = target_encounters.join(
        covid_master_df.select("patient_id"), 
        on="patient_id", 
        how="semi"
    )

    # Tính LOS = difference in days between start and stop
    hospitalization_df = covid_encounters.withColumn(
        "length_of_stay_days",
        F.datediff("encounter_stop", "encounter_start")
    )

    # Aggregation
    hospitalization_agg = (
        hospitalization_df
        .groupBy("encounter_description")
        .agg(
            F.countDistinct("patient_id").alias("patient_count"),
            F.round(F.avg("length_of_stay_days"), 2).alias("avg_length_of_stay_days")
        )
        .withColumn(
            "percentage",
            F.round((F.col("patient_count") / F.lit(total_covid_patients)) * 100, 2)
        )
        .orderBy("encounter_description")
    )

    hosp_result_count = hospitalization_agg.count()
    print(f"[INFO] Hospitalization logic completed. Result rows: {hosp_result_count}")
    hospitalization_agg.show(truncate=False)

    print(f"[INFO] Writing target table: {HOSPITALIZATION_TABLE}")
    hospitalization_agg.writeTo(HOSPITALIZATION_TABLE).createOrReplace()

    print("\n[CHECK] Hive Metastore metadata for target table:")
    print(f"  - table exists: {spark.catalog.tableExists(HOSPITALIZATION_TABLE)}")

    print("\n[DONE] Gold table aggregation completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
