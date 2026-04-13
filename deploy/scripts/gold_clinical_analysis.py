from __future__ import annotations
import os
from pyspark.sql import DataFrame, SparkSession, functions as F

# CONFIG từ biến môi trường của Airflow
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "lakehouse_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "Lakehouse123!")

CATALOG_NAME = "hospital"
BRONZE_NAMESPACE = f"{CATALOG_NAME}.bronze"
SILVER_NAMESPACE = f"{CATALOG_NAME}.silver"
GOLD_NAMESPACE = f"{CATALOG_NAME}.gold"

SILVER_MASTER = f"{SILVER_NAMESPACE}.covid_clinical_master"
BRONZE_ENCOUNTERS = f"{BRONZE_NAMESPACE}.encounters"
BRONZE_CONDITIONS = f"{BRONZE_NAMESPACE}.conditions"

TABLE_DEMOGRAPHICS = f"{GOLD_NAMESPACE}.mortality_by_demographics"
TABLE_PATHWAYS = f"{GOLD_NAMESPACE}.covid_pathway_summary"

def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("Iceberg Gold Clinical Analytics")
        .config("spark.sql.catalog.hospital", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hospital.type", "hadoop")
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

def create_demographics_mortality(spark: SparkSession):
    print("⏳ Đang phân tích Bảng 1: Phân tích tử vong theo Nhân Khẩu Học...")
    df_silver = spark.table(SILVER_MASTER)
    
    # 1. Tính tuổi (lấy năm hiện tại trừ năm sinh, hoặc năm mất trừ năm sinh)
    cur_year = F.year(F.current_date())
    age_col = F.when(F.col("patient_deathdate").isNotNull(), F.year("patient_deathdate") - F.year("patient_birthdate")) \
               .otherwise(cur_year - F.year("patient_birthdate"))
               
    df_with_age = df_silver.withColumn("age", age_col)
    
    # 2. Phân loại nhóm tuổi
    df_with_group = df_with_age.withColumn("age_group",
        F.when(F.col("age") <= 18, "0-18")
         .when((F.col("age") > 18) & (F.col("age") <= 34), "19-34")
         .when((F.col("age") > 34) & (F.col("age") <= 49), "35-49")
         .when((F.col("age") > 49) & (F.col("age") <= 64), "50-64")
         .when(F.col("age") >= 65, "65+")
         .otherwise("Unknown")
    )
    
    # 3. Aggregate
    df_agg = df_with_group.groupBy("age_group", "gender").agg(
        F.count("patient_id").alias("total_covid_patients"),
        F.sum(F.when(F.col("patient_deathdate").isNotNull(), 1).otherwise(0)).alias("total_deaths")
    )
    
    # 4. Tính tỷ lệ %
    df_final = df_agg.withColumn(
        "mortality_rate_percent", 
        F.round((F.col("total_deaths") / F.col("total_covid_patients")) * 100, 2)
    )
    
    # Ghi ra bảng Gold
    df_final.writeTo(TABLE_DEMOGRAPHICS).createOrReplace()
    print("✅ Đã tạo thành công bảng: " + TABLE_DEMOGRAPHICS)
    df_final.show(10)

def create_pathway_summary(spark: SparkSession):
    print("⏳ Đang phân tích Bảng 2: Thống kê Phân luồng điều trị (Pathways)...")
    df_silver = spark.table(SILVER_MASTER)
    df_encounters = spark.table(BRONZE_ENCOUNTERS)
    df_conditions = spark.table(BRONZE_CONDITIONS)
    
    # Danh sách tập bệnh nhân COVID
    covid_patients = df_silver.select("patient_id", "patient_deathdate")
    
    # Gắn flag tử vong / hồi phục
    covid_patients = covid_patients.withColumn("is_dead", F.when(F.col("patient_deathdate").isNotNull(), 1).otherwise(0))
    covid_patients = covid_patients.withColumn("is_recovered", F.when(F.col("patient_deathdate").isNull(), 1).otherwise(0))
    
    # Xét các luồng nhập viện / ICU dựa trên lịch sử Khám (Encounters)
    # Bronze encounters dùng cột 'patient', Silver dùng 'patient_id'
    encounters_flagged = df_encounters.join(
        covid_patients,
        df_encounters["patient"] == covid_patients["patient_id"],
        how="inner"
    ).withColumn(
        "is_hospitalized",
        F.when(F.lower(F.col("encounterclass")).like("%inpatient%"), 1).otherwise(0)
    ).withColumn(
        "is_icu",
        F.when(F.lower(F.col("encounterclass")).like("%icu%") | F.lower(F.col("encounterclass")).like("%intensive%"), 1).otherwise(0)
    )
    
    patient_enc_agg = encounters_flagged.groupBy(covid_patients["patient_id"]).agg(
        F.max("is_hospitalized").alias("hospitalized_flag"),
        F.max("is_icu").alias("icu_flag")
    )
    
    # Nội suy thở máy (Mechanical Ventilation) từ Conditions / Encounters description
    # Bronze conditions dùng cột 'patient', không phải 'patient_id'
    vent_cond = df_conditions.join(
        covid_patients,
        df_conditions["patient"] == covid_patients["patient_id"],
        how="inner"
    ).withColumn(
        "is_vent",
        F.when(F.lower(F.col("description")).like("%ventilator%") | 
               F.lower(F.col("description")).like("%ventilation%") | 
               F.lower(F.col("description")).like("%hypoxemia%"), 1).otherwise(0)
    )
    patient_vent_agg = vent_cond.groupBy(covid_patients["patient_id"]).agg(F.max("is_vent").alias("vent_flag"))
    
    # Tổng hợp tất cả về 1 bảng Master Passway
    master_pathway = covid_patients.join(patient_enc_agg, on="patient_id", how="left") \
                                   .join(patient_vent_agg, on="patient_id", how="left")
                                   
    master_pathway = master_pathway.fillna(0, subset=["hospitalized_flag", "icu_flag", "vent_flag"])
    
    # Cách ly tại nhà = Không có nhập viện
    master_pathway = master_pathway.withColumn("home_isolation_flag", F.when(F.col("hospitalized_flag") == 0, 1).otherwise(0))
    
    # Tóm tắt số liệu
    df_summary = master_pathway.agg(
        F.count("patient_id").alias("total_covid_cases"),
        F.sum("home_isolation_flag").alias("home_isolation_cases"),
        F.sum("hospitalized_flag").alias("hospitalized_cases"),
        F.sum("icu_flag").alias("icu_cases"),
        F.sum("vent_flag").alias("ventilation_cases"),
        F.sum("is_recovered").alias("recovered_cases"),
        F.sum("is_dead").alias("death_cases")
    )
    
    # Ghi ra bảng Gold
    df_summary.writeTo(TABLE_PATHWAYS).createOrReplace()
    print("✅ Đã tạo thành công bảng: " + TABLE_PATHWAYS)
    df_summary.show()

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    
    print("====================================")
    print(" Bắt đầu chạy Lớp GOLD ANALYTICS    ")
    print("====================================")
    
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}")
    
    try:
        create_demographics_mortality(spark)
        create_pathway_summary(spark)
    except Exception as e:
        print(f"❌ Xảy ra lỗi trong quá trình tính toán: {e}")
    finally:
        spark.stop()
        print("🎉 Hoàn tất tiến trình lớp Vàng (Gold)!")

if __name__ == "__main__":
    main()
