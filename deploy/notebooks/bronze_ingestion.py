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
        .config("spark.sql.catalog.hospital.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "lakehouse_admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "Lakehouse123!") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.defaultCatalog", "hospital") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    print("✅ Đã kết nối Spark. Tiến hành đọc dữ liệu Raw (Parquet) từ MinIO...")
    # 2. Đọc dữ liệu thô (từ các folder Parquet đã được đẩy ở Task 1.4)
    try:
        # Chú ý: Đọc dạng parquet từ thư mục
        patients_df = spark.read.parquet("s3a://hospital-lakehouse/raw/patients")
        encounters_df = spark.read.parquet("s3a://hospital-lakehouse/raw/encounters")
        conditions_df = spark.read.parquet("s3a://hospital-lakehouse/raw/conditions")
        
        print(f"📊 Thống kê sơ bộ: Patients({patients_df.count()}), Encounters({encounters_df.count()}), Conditions({conditions_df.count()})")
    except Exception as e:
        print("❌ Lỗi khi đọc dữ liệu raw từ MinIO: Vui lòng đảm bảo folder Parquet đã tồn tại trong bucket hospital-lakehouse/raw/")
        print("Lỗi chi tiết:", str(e))
        spark.stop()
        return

    # 3. Khởi tạo Database/Namespace bronze nằm trong catalog
    print("🛠 Đang làm sạch và khởi tạo Namespace 'bronze'...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hospital.bronze")
    
    # Xóa bảng cũ nếu tồn tại để tránh lỗi xung đột kiểu dữ liệu (Schema Evolution)
    spark.sql("DROP TABLE IF EXISTS hospital.bronze.patients")
    spark.sql("DROP TABLE IF EXISTS hospital.bronze.encounters")
    spark.sql("DROP TABLE IF EXISTS hospital.bronze.conditions")

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
