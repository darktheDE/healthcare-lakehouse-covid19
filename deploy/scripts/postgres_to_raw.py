"""
postgres_to_raw.py
──────────────────
Đọc 3 bảng patients / encounters / conditions từ PostgreSQL (postgres-source)
và ghi ra MinIO bucket hospital-lakehouse/raw/ dưới dạng Parquet.

Chạy độc lập bằng PySpark bên trong Docker network iceberg_net.
"""

import os
from pyspark.sql import SparkSession

# ─── CONFIG ─────────────────────────────────────────────────────────────────
PG_HOST     = os.getenv("PG_HOST",     "postgres-source")
PG_PORT     = os.getenv("PG_PORT",     "5432")
PG_DB       = os.getenv("PG_DB",       "hospital_db")
PG_USER     = os.getenv("PG_USER",     "admin")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID",     "admin")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
MINIO_BUCKET     = "hospital-lakehouse"
RAW_PATH         = f"s3a://{MINIO_BUCKET}/raw"

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_DRIVER = "org.postgresql.Driver"

TABLES = ["patients", "encounters", "conditions"]
# ────────────────────────────────────────────────────────────────────────────


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("postgres-to-raw")
        # ── TẮT REST catalog mặc định của tabulario image ──
        # (image mặc định trỏ tới http://rest:8181 không tồn tại)
        .config("spark.sql.catalog.demo",                     "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type",                "hadoop")
        .config("spark.sql.catalog.demo.warehouse",           f"s3a://{MINIO_BUCKET}/warehouse")
        # ── S3A / MinIO ──
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # ── tắt SSL check cho MinIO self-signed ──
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ── tắt SparkUI để nhẹ hơn ──
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def read_table(spark: SparkSession, table: str):
    print(f"[INFO] Đọc bảng '{table}' từ PostgreSQL...")
    return (
        spark.read
        .format("jdbc")
        .option("url",      JDBC_URL)
        .option("dbtable",  table)
        .option("user",     PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver",   JDBC_DRIVER)
        # Tăng tốc đọc bảng lớn (encounters ~3M rows)
        .option("fetchsize", "10000")
        .load()
    )


def write_raw(df, table: str):
    dest = f"{RAW_PATH}/{table}"
    print(f"[INFO] Ghi {df.count()} dòng  →  {dest}/")
    (
        df.write
        .mode("overwrite")
        .parquet(dest)
    )
    print(f"[OK]  Hoàn thành bảng '{table}'")


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  Postgres → MinIO raw/   (hospital-lakehouse)")
    print("=" * 60)

    for table in TABLES:
        df = read_table(spark, table)
        write_raw(df, table)

    spark.stop()
    print("\n[DONE] Tất cả bảng đã được đẩy lên raw/")


if __name__ == "__main__":
    main()
