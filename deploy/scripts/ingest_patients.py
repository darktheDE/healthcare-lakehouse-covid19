import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Mapping CSV header → PostgreSQL column name
PATIENTS_RENAME = {
    "Id": "id",
    "BIRTHDATE": "birthdate",
    "DEATHDATE": "deathdate",
    "SSN": "ssn",
    "DRIVERS": "drivers",
    "PASSPORT": "passport",
    "PREFIX": "prefix",
    "FIRST": "first",
    "LAST": "last",
    "SUFFIX": "suffix",
    "MAIDEN": "maiden",
    "MARITAL": "marital",
    "RACE": "race",
    "ETHNICITY": "ethnicity",
    "GENDER": "gender",
    "BIRTHPLACE": "birthplace",
    "ADDRESS": "address",
    "CITY": "city",
    "STATE": "state",
    "COUNTY": "county",
    "ZIP": "zip",
    "LAT": "lat",
    "LON": "lon",
    "HEALTHCARE_EXPENSES": "healthcare_expenses",
    "HEALTHCARE_COVERAGE": "healthcare_coverage",
}


def truncate_table(pg_host, pg_db, pg_user, pg_pass, table: str, cascade: bool = False):
    conn = psycopg2.connect(host=pg_host, port=5432, dbname=pg_db, user=pg_user, password=pg_pass)
    conn.autocommit = True
    suffix = " CASCADE" if cascade else ""
    conn.cursor().execute(f"TRUNCATE TABLE {table}{suffix};")
    conn.close()
    print(f"[INFO] TRUNCATE {table}{suffix} done.")


def main():
    spark = (
        SparkSession.builder.appName("Ingest-Patients")
        .config("spark.sql.sources.default", "csv")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    pg_host = os.getenv("PG_HOST", "postgres-source")
    pg_db   = os.getenv("PG_DB",   "hospital_db")
    pg_user = os.getenv("PG_USER", "admin")
    pg_pass = os.getenv("PG_PASSWORD", "password")
    jdbc_url = f"jdbc:postgresql://{pg_host}:5432/{pg_db}"
    conn_props = {
        "user": pg_user, "password": pg_pass,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified",
    }

    path = "/data_source/patients.csv"
    print(f"[INFO] Đọc CSV: {path}")
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    )

    # Rename columns CSV → PostgreSQL schema
    for csv_col, pg_col in PATIENTS_RENAME.items():
        if csv_col in df.columns:
            df = df.withColumnRenamed(csv_col, pg_col)

    total = df.count()
    print(f"[INFO] Số dòng đọc từ CSV: {total}")
    print(f"[INFO] Schema sau rename: {df.columns}")

    # TRUNCATE CASCADE (patients là parent của encounters + conditions)
    truncate_table(pg_host, pg_db, pg_user, pg_pass, "patients", cascade=True)

    print("[INFO] Ghi vào PostgreSQL bảng 'patients'...")
    df.write.jdbc(url=jdbc_url, table="patients", mode="append", properties=conn_props)
    print(f"[DONE] patients: {total} rows ingested successfully.")
    spark.stop()


if __name__ == "__main__":
    main()