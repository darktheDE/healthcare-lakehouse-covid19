import os
from pyspark.sql import SparkSession

# CONFIG from environment variables
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "lakehouse_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "Lakehouse123!")



def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Iceberg Bronze Ingestion")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.hospital",                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hospital.type",           "hive")
        .config("spark.sql.catalog.hospital.uri",            "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.hospital.warehouse",      "s3a://hospital-lakehouse/warehouse")
        .config("spark.hadoop.fs.s3a.endpoint",              MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",            MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",            MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",     "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
        .config("spark.hadoop.fs.s3a.impl",                  "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.defaultCatalog", "hospital")
        .getOrCreate()
    )


def write_iceberg(df, table: str, spark: SparkSession):
    """
    Ghi DataFrame vào Iceberg table.
    - Nếu table đã tồn tại: overwrite toàn bộ dữ liệu.
    - Nếu chưa tồn tại: tạo mới.
    Không dùng DROP TABLE để tránh lỗi metadata khi createOrReplace.
    """
    # Thử createOrReplace trực tiếp (không DROP trước)
    try:
        df.writeTo(table).using("iceberg").createOrReplace()
    except Exception as e:
        print(f"[WARN] createOrReplace failed ({e}), thử write.format overwrite...")
        # Fallback: ghi bằng DataFrameWriter với format iceberg
        df.write \
          .format("iceberg") \
          .mode("overwrite") \
          .option("overwrite-mode", "dynamic") \
          .saveAsTable(table)


def main():
    print("🚀 Bắt đầu khởi tạo SparkSession...")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("✅ Đã kết nối Spark. Tiến hành đọc dữ liệu Raw (Parquet) từ MinIO...")
    try:
        patients_df   = spark.read.parquet("s3a://hospital-lakehouse/raw/patients")
        encounters_df = spark.read.parquet("s3a://hospital-lakehouse/raw/encounters")
        conditions_df = spark.read.parquet("s3a://hospital-lakehouse/raw/conditions")

        p_count = patients_df.count()
        e_count = encounters_df.count()
        c_count = conditions_df.count()
        print(f"📊 Thống kê: Patients({p_count}), Encounters({e_count}), Conditions({c_count})")
    except Exception as e:
        print("❌ Lỗi đọc raw từ MinIO:", str(e))
        spark.stop()
        return

    print("🛠 Khởi tạo Namespace 'bronze'...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hospital.bronze")

    print("⏳ Ghi bảng Patients vào Bronze (Iceberg)...")
    write_iceberg(patients_df, "hospital.bronze.patients", spark)
    print(f"✅ Bronze.patients: {p_count} rows OK")

    print("⏳ Ghi bảng Encounters vào Bronze (Iceberg)...")
    write_iceberg(encounters_df, "hospital.bronze.encounters", spark)
    print(f"✅ Bronze.encounters: {e_count} rows OK")

    print("⏳ Ghi bảng Conditions vào Bronze (Iceberg)...")
    write_iceberg(conditions_df, "hospital.bronze.conditions", spark)
    print(f"✅ Bronze.conditions: {c_count} rows OK")

    print("🎉 Hoàn tất quá trình Ingestion vào lớp Bronze!")
    spark.stop()


if __name__ == "__main__":
    main()
