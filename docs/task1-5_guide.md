# Task 1.5 Guide: Bronze Layer Ingestion (Iceberg)

Tài liệu này hướng dẫn cách nạp dữ liệu từ lớp Raw vào các bảng Apache Iceberg thuộc lớp Bronze và cách truy cập dữ liệu qua Trino.

## 1. Quy trình xử lý
- **Input:** Folder Parquet từ `s3a://hospital-lakehouse/raw/`
- **Logic:**
  1. Đọc dữ liệu Parquet bằng PySpark.
  2. Tự động dọn dẹp các bảng cũ để tránh xung đột kiểu dữ liệu.
  3. Ghi dữ liệu vào Catalog Iceberg sử dụng Hive Metastore.
- **Output:** Bảng Iceberg tại `s3a://hospital-lakehouse/bronze.db/`

## 2. Cách thực hiện
Kích hoạt job nạp Bronze:
```bash
docker compose --env-file .env -f deploy/docker-compose.yml up spark-bronze-job
```

## 3. Cấu hình Trino (Cực kỳ quan trọng)
Để truy vấn được lớp Bronze qua Trino 480, cấu hình tại `deploy/trino/catalog/iceberg.properties` phải tuân thủ:
- `iceberg.catalog.type=HIVE_METASTORE`
- `fs.native-s3.enabled=true`
- Sử dụng prefix `s3.aws-access-key` và `s3.aws-secret-key`.

## 4. Truy vấn kiểm tra (Verification)
Sử dụng Trino CLI để xác nhận 4.4 triệu dòng dữ liệu:

```sql
-- Kiểm tra các bảng
SHOW TABLES IN iceberg.bronze;

-- Đếm tổng số bản ghi
SELECT count(*) FROM iceberg.bronze.patients;
SELECT count(*) FROM iceberg.bronze.encounters;
SELECT count(*) FROM iceberg.bronze.conditions;
```

## 5. Thống kê dữ liệu hiện tại (Tham chiếu)
| Table      | Record Count | Layer  | Format  |
|------------|--------------|--------|---------|
| Patients   | 124,150      | Bronze | Iceberg |
| Conditions | 1,143,900    | Bronze | Iceberg |
| Encounters | 3,188,675    | Bronze | Iceberg |

---
*Cập nhật: 10/04/2026*
