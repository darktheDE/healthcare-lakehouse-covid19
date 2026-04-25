import os
import psycopg2
from pyspark.sql import SparkSession


# CSV header → PostgreSQL column name
ENCOUNTERS_RENAME = {
    "Id": "id",
    "START": "start_time",
    "STOP": "stop_time",
    "PATIENT": "patient",
    "ORGANIZATION": "organization",
    "PROVIDER": "provider",
    "PAYER": "payer",
    "ENCOUNTERCLASS": "encounterclass",
    "CODE": "code",
    "DESCRIPTION": "description",
    "BASE_ENCOUNTER_COST": "base_encounter_cost",
    "TOTAL_CLAIM_COST": "total_claim_cost",
    "PAYER_COVERAGE": "payer_coverage",
    "REASONCODE": "reasoncode",
    "REASONDESCRIPTION": "reasondescription",
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
        SparkSession.builder.appName("Ingest-Encounters")
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

    path = "/data_source/encounters.csv"
    print(f"[INFO] Đọc CSV: {path}")
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    )

    for csv_col, pg_col in ENCOUNTERS_RENAME.items():
        if csv_col in df.columns:
            df = df.withColumnRenamed(csv_col, pg_col)

    total = df.count()
    print(f"[INFO] Số dòng đọc từ CSV: {total}")
    print(f"[INFO] Schema sau rename: {df.columns}")

    truncate_table(pg_host, pg_db, pg_user, pg_pass, "encounters", cascade=True)

    print("[INFO] Ghi vào PostgreSQL bảng 'encounters'...")
    df.write.jdbc(url=jdbc_url, table="encounters", mode="append", properties=conn_props)
    print(f"[DONE] encounters: {total} rows ingested successfully.")
    spark.stop()


if __name__ == "__main__":
    main()