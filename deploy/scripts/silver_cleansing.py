from __future__ import annotations
import os
from pyspark.sql import DataFrame, SparkSession, functions as F

# CONFIG from environment variables
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "lakehouse_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "Lakehouse123!")

CATALOG_NAME = "hospital"
BRONZE_NAMESPACE = f"{CATALOG_NAME}.bronze"
SILVER_NAMESPACE = f"{CATALOG_NAME}.silver"
OUTPUT_TABLE = f"{SILVER_NAMESPACE}.covid_clinical_master"

BRONZE_PATIENTS = f"{BRONZE_NAMESPACE}.patients"
BRONZE_ENCOUNTERS = f"{BRONZE_NAMESPACE}.encounters"
BRONZE_CONDITIONS = f"{BRONZE_NAMESPACE}.conditions"
BRONZE_OBSERVATIONS = f"{BRONZE_NAMESPACE}.observations"

def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("Iceberg Silver Cleansing")
        .config("spark.sql.catalog.hospital", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hospital.type", "hive")
        .config("spark.sql.catalog.hospital.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.hospital.warehouse", "s3a://hospital-lakehouse/warehouse")
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
        .getOrCreate()
    )

def normalize_identifier(column: F.Column) -> F.Column:
    return F.when(F.trim(column).isin("", "NULL", "null"), F.lit(None)).otherwise(F.trim(column))

def normalize_gender(column: F.Column) -> F.Column:
    normalized = F.upper(F.trim(column))
    return (
        F.when(normalized == F.lit("M"), F.lit("Male"))
        .when(normalized == F.lit("F"), F.lit("Female"))
        .when(F.trim(column).isNull() | (F.trim(column) == ""), F.lit(None))
        .otherwise(F.initcap(F.lower(F.trim(column))))
    )

def wait_for_table(
    spark: SparkSession,
    table_name: str,
    required: bool = True,
) -> DataFrame | None:
    try:
        return spark.table(table_name)
    except Exception as exc:
        if not required:
            return None
        raise RuntimeError(f"Table {table_name} was not ready") from exc

def clean_patients(patients: DataFrame) -> DataFrame:
    return (
        patients.select(
            normalize_identifier(F.col("id")).alias("patient_id"),
            F.to_date(F.col("birthdate")).alias("birthdate"),
            F.to_date(F.col("deathdate")).alias("deathdate"),
            F.col("ssn").cast("string").alias("ssn"),
            F.col("drivers").cast("string").alias("drivers"),
            F.col("passport").cast("string").alias("passport"),
            F.col("prefix").cast("string").alias("prefix"),
            F.col("first").cast("string").alias("first_name"),
            F.col("last").cast("string").alias("last_name"),
            F.col("suffix").cast("string").alias("suffix"),
            F.col("maiden").cast("string").alias("maiden_name"),
            F.col("marital").cast("string").alias("marital_status"),
            F.col("race").cast("string").alias("race"),
            F.col("ethnicity").cast("string").alias("ethnicity"),
            normalize_gender(F.col("gender")).alias("gender"),
            F.col("birthplace").cast("string").alias("birthplace"),
            F.col("address").cast("string").alias("address"),
            F.col("city").cast("string").alias("city"),
            F.col("state").cast("string").alias("state"),
            F.col("county").cast("string").alias("county"),
            F.col("zip").cast("string").alias("zip_code"),
            F.col("lat").cast("decimal(10, 6)").alias("lat"),
            F.col("lon").cast("decimal(10, 6)").alias("lon"),
            F.col("healthcare_expenses").cast("decimal(12, 2)").alias("healthcare_expenses"),
            F.col("healthcare_coverage").cast("decimal(12, 2)").alias("healthcare_coverage"),
        )
        .filter(F.col("patient_id").isNotNull())
        .dropDuplicates(["patient_id"])
    )

def clean_encounters(encounters: DataFrame) -> DataFrame:
    return (
        encounters.select(
            normalize_identifier(F.col("id")).alias("encounter_id"),
            F.to_timestamp(F.col("start_time")).alias("encounter_start"),
            F.to_timestamp(F.col("stop_time")).alias("encounter_stop"),
            normalize_identifier(F.col("patient")).alias("patient_id"),
            normalize_identifier(F.col("organization")).alias("organization_id"),
            normalize_identifier(F.col("provider")).alias("provider_id"),
            normalize_identifier(F.col("payer")).alias("payer_id"),
            F.col("encounterclass").cast("string").alias("encounter_class"),
            F.col("code").cast("string").alias("encounter_code"),
            F.col("description").cast("string").alias("encounter_description"),
            F.col("base_encounter_cost").cast("decimal(12, 2)").alias("base_encounter_cost"),
            F.col("total_claim_cost").cast("decimal(12, 2)").alias("total_claim_cost"),
            F.col("payer_coverage").cast("decimal(12, 2)").alias("payer_coverage"),
            F.col("reasoncode").cast("string").alias("reason_code"),
            F.col("reasondescription").cast("string").alias("reason_description"),
        )
        .filter(
            F.col("encounter_id").isNotNull()
            & F.col("patient_id").isNotNull()
        )
        .dropDuplicates(["encounter_id"])
    )

def clean_conditions(conditions: DataFrame) -> DataFrame:
    return (
        conditions.select(
            normalize_identifier(F.col("patient")).alias("patient_id"),
            normalize_identifier(F.col("encounter")).alias("encounter_id"),
            F.to_date(F.col("start_date")).alias("condition_start_date"),
            F.to_date(F.col("stop_date")).alias("condition_stop_date"),
            F.col("code").cast("string").alias("condition_code"),
            F.col("description").cast("string").alias("condition_description"),
        )
        .filter(
            F.col("patient_id").isNotNull()
            & F.col("encounter_id").isNotNull()
        )
        .dropDuplicates(
            [
                "patient_id",
                "encounter_id",
                "condition_code",
                "condition_start_date",
                "condition_stop_date",
                "condition_description",
            ]
        )
    )

def clean_observations(observations: DataFrame) -> DataFrame:
    cols = {c.lower(): c for c in observations.columns}
    date_col = cols.get("date", cols.get("start", cols.get("start_date", None)))
    return (
        observations.select(
            normalize_identifier(F.col(cols.get("patient", "patient"))).alias("patient_id"),
            normalize_identifier(F.col(cols.get("encounter", "encounter"))).alias("encounter_id"),
            (F.to_timestamp(F.col(date_col)) if date_col else F.lit(None).cast("timestamp")).alias(
                "observation_time"
            ),
            F.col(cols.get("code", "code")).cast("string").alias("observation_code"),
            F.col(cols.get("description", "description")).cast("string").alias("observation_description"),
            F.col(cols.get("value", "value")).cast("string").alias("observation_value"),
        )
        .filter(F.col("patient_id").isNotNull())
        .dropDuplicates(["patient_id", "encounter_id", "observation_time", "observation_code", "observation_description", "observation_value"])
    )

def build_covid_clinical_master(
    patients: DataFrame,
    encounters: DataFrame,
    conditions: DataFrame,
    observations: DataFrame | None,
) -> DataFrame:
    covid_from_conditions = (
        conditions.filter(F.col("condition_code") == F.lit("840539006"))
        .select("patient_id")
        .distinct()
    )

    observation_summary = None
    covid_from_observations = None
    if observations is not None:
        positive_flag = (
            F.upper(F.coalesce(F.col("observation_value"), F.lit(""))).rlike(
                "POSITIVE|DETECTED|REACTIVE"
            )
            | F.upper(F.coalesce(F.col("observation_description"), F.lit(""))).contains("POSITIVE")
        )
        covid_from_observations = observations.filter(positive_flag).select("patient_id").distinct()

    covid_patients = covid_from_conditions
    if covid_from_observations is not None:
        covid_patients = covid_patients.unionByName(covid_from_observations).distinct()

    conditions_covid = conditions.join(covid_patients, on="patient_id", how="semi")
    encounters_covid = encounters.join(covid_patients, on="patient_id", how="semi")

    condition_summary = (
        conditions_covid.repartition("patient_id")
        .groupBy("patient_id")
        .agg(
            F.count(F.lit(1)).alias("condition_count"),
            F.max(F.col("condition_start_date")).alias("last_condition_date"),
        )
    )

    encounter_summary = (
        encounters_covid.repartition("patient_id")
        .groupBy("patient_id")
        .agg(
            F.count(F.lit(1)).alias("encounter_count"),
            F.max(F.col("encounter_start")).alias("last_encounter_time"),
            F.sum(F.coalesce(F.col("total_claim_cost"), F.lit(0))).cast("decimal(18,2)").alias(
                "total_claim_cost_sum"
            ),
        )
    )

    if observations is not None:
        observation_summary = (
            observations.join(covid_patients, on="patient_id", how="semi")
            .repartition("patient_id")
            .groupBy("patient_id")
            .agg(
                F.count(F.lit(1)).alias("observation_count"),
                F.max(F.col("observation_time")).alias("last_observation_time"),
            )
        )

    master = (
        patients.alias("p")
        .join(covid_patients.alias("covid"), F.col("p.patient_id") == F.col("covid.patient_id"), "inner")
        .join(encounter_summary.alias("e"), F.col("p.patient_id") == F.col("e.patient_id"), "left")
        .join(condition_summary.alias("c"), F.col("p.patient_id") == F.col("c.patient_id"), "left")
    )

    if observation_summary is not None:
        master = master.join(
            observation_summary.alias("o"), F.col("p.patient_id") == F.col("o.patient_id"), "left"
        )

    select_columns = [
        F.col("p.patient_id"), F.col("p.birthdate").alias("patient_birthdate"),
        F.col("p.deathdate").alias("patient_deathdate"), F.col("p.ssn"), F.col("p.drivers"), F.col("p.passport"),
        F.col("p.prefix"), F.col("p.first_name"), F.col("p.last_name"), F.col("p.suffix"), F.col("p.maiden_name"),
        F.col("p.marital_status"), F.col("p.race"), F.col("p.ethnicity"), F.col("p.gender"), F.col("p.birthplace"),
        F.col("p.address"), F.col("p.city"), F.col("p.state"), F.col("p.county"), F.col("p.zip_code"),
        F.col("p.lat"), F.col("p.lon"), F.col("p.healthcare_expenses"), F.col("p.healthcare_coverage"),
        F.coalesce(F.col("e.encounter_count"), F.lit(0)).alias("encounter_count"),
        F.col("e.last_encounter_time"),
        F.coalesce(F.col("e.total_claim_cost_sum"), F.lit(0).cast("decimal(18,2)")).alias("total_claim_cost_sum"),
        F.coalesce(F.col("c.condition_count"), F.lit(0)).alias("condition_count"),
        F.col("c.last_condition_date"),
    ]

    if observation_summary is not None:
        select_columns.extend([
            F.coalesce(F.col("o.observation_count"), F.lit(0)).alias("observation_count"),
            F.col("o.last_observation_time"),
        ])
    else:
        select_columns.extend([
            F.lit(0).alias("observation_count"),
            F.lit(None).cast("timestamp").alias("last_observation_time"),
        ])

    return master.select(*select_columns)

def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("[INFO] Waiting for Bronze tables to be ready...")
    patients_bronze = wait_for_table(spark, BRONZE_PATIENTS)
    encounters_bronze = wait_for_table(spark, BRONZE_ENCOUNTERS)
    conditions_bronze = wait_for_table(spark, BRONZE_CONDITIONS)
    observations_bronze = wait_for_table(spark, BRONZE_OBSERVATIONS, required=False)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NAMESPACE}")

    print("[INFO] Cleaning source tables...")
    patients_clean = clean_patients(patients_bronze)
    encounters_clean = clean_encounters(encounters_bronze)
    conditions_clean = clean_conditions(conditions_bronze)
    observations_clean = clean_observations(observations_bronze) if observations_bronze is not None else None

    print(f"[INFO] Clean rows - patients: {patients_clean.count()}")
    covid_clinical_master = build_covid_clinical_master(
        patients_clean, encounters_clean, conditions_clean, observations_clean,
    )

    print(f"[INFO] COVID clinical master rows: {covid_clinical_master.count()}")
    
    print(f"[INFO] Writing Silver tables...")
    patients_clean.writeTo(f"{SILVER_NAMESPACE}.patients").createOrReplace()
    encounters_clean.writeTo(f"{SILVER_NAMESPACE}.encounters").createOrReplace()
    conditions_clean.writeTo(f"{SILVER_NAMESPACE}.conditions").createOrReplace()
    covid_clinical_master.writeTo(OUTPUT_TABLE).createOrReplace()

    print("[DONE] Silver table enrichment completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
