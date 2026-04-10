from pyspark.sql import SparkSession

def main():
    print("🚀 Bắt đầu khởi tạo SparkSession...")
    # 1. Cấu hình Spark Session kết nối với Iceberg và MinIO
    spark = SparkSession.builder \
        .appName("Iceberg Bronze Ingestion") \
        .config("spark.sql.catalog.hospital", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hospital.type", "hive") \
        .config("spark.sql.catalog.hospital.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.hospital.warehouse", "s3a://hospital-lakehouse/") \
        .config("spark.sql.catalog.hospital.s3.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "lakehouse_admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "Lakehouse123!") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.defaultCatalog", "hospital") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    print("✅ Đã kết nối Spark. Tiến hành đọc dữ liệu Raw từ MinIO...")
    # 2. Đọc dữ liệu thô (từ các file CSV đã có trong s3a://hospital-lakehouse/raw/)
    # Giả định ở bước trước dữ liệu thô đã được đẩy vào /raw/ dưới định dạng csv
    try:
        patients_df = spark.read.option("header", "true").option("inferSchema", "true").csv("s3a://hospital-lakehouse/raw/patients.csv")
        encounters_df = spark.read.option("header", "true").option("inferSchema", "true").csv("s3a://hospital-lakehouse/raw/encounters.csv")
        conditions_df = spark.read.option("header", "true").option("inferSchema", "true").csv("s3a://hospital-lakehouse/raw/conditions.csv")
    except Exception as e:
        print("❌ Lỗi khi đọc dữ liệu raw từ MinIO: Vui lòng đảm bảo các file CSV đã tồn tại trong bucket hospital-lakehouse/raw/")
        print("Lỗi chi tiết:", str(e))
        spark.stop()
        return

    # 3. Khởi tạo Database/Namespace bronze nằm trong catalog
    print("🛠 Đang khởi tạo Namespace 'bronze' trên Iceberg Catalog...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hospital.bronze")

    # 4. Ghi dữ liệu vào định dạng Iceberg thông qua hàm writeTo
    print("⏳ Đang ghi dữ liệu bảng Patients vào Bronze...")
    patients_df.writeTo("hospital.bronze.patients").createOrReplace()
    
    print("⏳ Đang ghi dữ liệu bảng Encounters vào Bronze...")
    encounters_df.writeTo("hospital.bronze.encounters").createOrReplace()
    
    print("⏳ Đang ghi dữ liệu bảng Conditions vào Bronze...")
    conditions_df.writeTo("hospital.bronze.conditions").createOrReplace()

    print("🎉 Hoàn tất quá trình Ingestion vào lớp Bronze!")
    spark.stop()

if __name__ == "__main__":
    main()
