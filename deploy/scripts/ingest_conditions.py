import os
import psycopg2
from pyspark.sql import SparkSession


# CSV header → PostgreSQL column name
# CSV: START,STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION (no 'id' column in conditions)
CONDITIONS_RENAME = {
    "START": "start_date",
    "STOP": "stop_date",
    "PATIENT": "patient",
    "ENCOUNTER": "encounter",
    "CODE": "code",
    "DESCRIPTION": "description",
}


def truncate_table(pg_host, pg_db, pg_user, pg_pass, table: str):
    conn = psycopg2.connect(host=pg_host, port=5432, dbname=pg_db, user=pg_user, password=pg_pass)
    conn.autocommit = True
    conn.cursor().execute(f"TRUNCATE TABLE {table};")
    conn.close()
    print(f"[INFO] TRUNCATE {table} done.")


def main():
    spark = (
        SparkSession.builder.appName("Ingest-Conditions")
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

    path = "/data_source/conditions.csv"
    print(f"[INFO] Đọc CSV: {path}")
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    )

    for csv_col, pg_col in CONDITIONS_RENAME.items():
        if csv_col in df.columns:
            df = df.withColumnRenamed(csv_col, pg_col)

    total = df.count()
    print(f"[INFO] Số dòng đọc từ CSV: {total}")
    print(f"[INFO] Schema sau rename: {df.columns}")

    # conditions không có FK constraint đi ra ngoài → truncate bình thường
    truncate_table(pg_host, pg_db, pg_user, pg_pass, "conditions")

    print("[INFO] Ghi vào PostgreSQL bảng 'conditions'...")
    df.write.jdbc(url=jdbc_url, table="conditions", mode="append", properties=conn_props)
    print(f"[DONE] conditions: {total} rows ingested successfully.")
    spark.stop()


if __name__ == "__main__":
    main()