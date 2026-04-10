# Hướng Dẫn Tích Hợp Spark-Iceberg (Ingestion Bronze Layer) - Task 1.5

Trong kiến trúc Medallion Lakehouse, bước này đảm nhận việc kéo dữ liệu thô (Raw) từ MinIO và chuyển đổi (Ingest) thành định dạng bảng Apache Iceberg có cấu trúc, lưu xuống vùng Database `bronze/`.

Tài liệu này cung cấp 2 phương pháp tiếp cận: Thực thi chạy tay để theo dõi trực tiếp và Tự động hóa thông qua Docker Compose.

---

## PHẦN 1: THỰC THI THỦ CÔNG (MANUAL CHECK)
Dành cho mục đích debug, theo dõi log lỗi thực tế hoặc muốn chủ động chạy từng script theo ý muốn.

### Bước 1: Chuẩn bị Script PySpark
Mã nguồn ingestion được tổ chức tại file `deploy/notebooks/bronze_ingestion.py`. Kịch bản xử lý bao gồm:
1. Đăng ký Catalog `hospital` và trỏ vào `thrift://hive-metastore:9083`.
2. Ghi đè chỉ định `spark.sql.defaultCatalog` để vá lỗ hổng tương thích với hệ thống Tabulario.
3. Kéo dữ liệu CSV Raw sử dụng API Hadoop S3A.
4. Ghi đè tạo bảng Iceberg qua cú pháp `.writeTo("hospital.bronze.<table_name>").createOrReplace()`.

### Bước 2: Kích hoạt Spark Job
Trên cửa sổ Terminal tại thư mục dự án gốc (host máy tính), bạn chạy phát súng sau:

```bash
docker exec -it spark-iceberg bash -c "spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /home/iceberg/notebooks/notebooks/bronze_ingestion.py"
```

*Cơ chế:* Job sẽ phân loại dependencies bị thiếu, trích xuất hai gói `hadoop-aws.jar` và `aws-java-sdk` từ nền tảng Maven, sau đó dịch mã nguồn để Ingest lên hệ sinh thái Data Lake.

---

## PHẦN 2: TỰ ĐỘNG HÓA HOÀN TOÀN AUTO-RUN (THÊM VÀO DOCKER-COMPOSE)
Phương án biến Job thành luồng Auto 100% khi một đồng nghiệp mới clone dự án về và gõ Start, hoàn toàn loại bỏ yếu tố Gõ Lệnh của con người.

### Cập nhật cấu hình Docker-compose:
Kịch bản là bổ sung một Service siêu nhẹ đóng vai trò như một Job Trigger. Bạn copy tệp mã sau thêm vào tệp `deploy/docker-compose.yml`:

```yaml
  spark-bronze-job:
    image: tabulario/spark-iceberg:latest
    container_name: spark-bronze-job
    networks:
      - iceberg_net
    depends_on:
      - hive-metastore
      - spark-iceberg
    volumes:
      - ./notebooks:/home/iceberg/notebooks/notebooks
    environment:
      - AWS_ACCESS_KEY_ID=$${MINIO_ROOT_USER}
      - AWS_SECRET_ACCESS_KEY=$${MINIO_ROOT_PASSWORD}
      - AWS_REGION=us-east-1
    command: >
      bash -c "
      echo 'Chờ hệ thống ổn định 15 giây...';
      sleep 15;
      spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /home/iceberg/notebooks/notebooks/bronze_ingestion.py
      "
```

### Cách vận hành:
Thành viên trong nhóm giờ đây chỉ cần khởi chạy hệ thống Data Lake bằng một lệnh bao quát toàn cục:
```bash
docker-compose --env-file ../.env -f deploy/docker-compose.yml up -d
```
Container `spark-bronze-job` sẽ tự khởi động, chờ MinIO online, submit tự động đống File CSV vào Iceberg. Sau khi "hoàn thành nhiệm vụ", Container sẽ tự Exit (Tắt) – Trả lại RAM cho toàn bộ cục diện máy chủ cực kỳ hiệu quả!

---

## PHẦN 3: ĐỀ XUẤT MỞ RỘNG ICEBERG (SCALABILITY)
Giúp team bạn nâng cấp tư duy thiết kế kho dữ liệu chuẩn mực để ghi điểm cộng trong dự án:

1. **Phân Vùng Dữ Liệu Tối Ưu (Auto Partitioning):**
   Bảng `encounters` rất nặng (1GB). Chút nữa khi đẩy tiếp sang lớp Silver, bạn không nên dùng `createOrReplace` trần lụi nữa, mà hãy sử dụng tính năng ẩn giấu Partition của Iceberg.
   Ví dụ trong code Spark: `.partitionedBy("encounterclass")`, Iceberg metadata sẽ tự động xẻ cây dữ liệu nhằm tăng tốc độ truy vấn gấp chục lần.
   
2. **Loại bỏ Trigger Chay - Dùng DAG Apache Airflow:**
   Chúng ta đang dùng Docker Container init (spark-bronze-job) để chạy. Việc này tuy dễ cấu hình nhưng thiếu tính Giám sát (Monitoring). Khuyến khích team viết Pipeline DAG đập vào giao diện Apache Airflow (Web UI `localhost:8082`), kết hợp dùng Operator như `SparkSubmitOperator` để lịch trình cập nhật lớp Bronze diễn ra mỗi tiếng một lần.

3. **Cơ Chế Update Delta (Upsert Merge Into):**
   Trong thực tế, Dữ liệu bệnh viện là sinh sôi theo thời gian. Lớp Bronze có thể ghi `createOrReplace` cho lẹ, nhưng ở các Pipeline Silver/Gold, bạn phải code thuật toán `MERGE INTO` (so sánh khóa chính `Id` của Encounters) để chỉ chèn dòng mới (Insert) và cập nhật dòng cũ (Update) – Tránh tình trạng nhai lại hàng Triệu Data cũ kỹ!
