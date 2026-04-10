# Task 1.4 Guide: Ingestion Source → Raw Layer

Tài liệu này hướng dẫn quy trình trích xuất dữ liệu từ nguồn PostgreSQL sang hồ chứa dữ liệu thô (Raw Layer) trên MinIO bằng PySpark.

## 1. Kiến trúc luồng dữ liệu
- **Source:** PostgreSQL (Container: `postgres-source`)
- **Destination:** MinIO Bucket: `hospital-lakehouse`, Folder: `raw/`
- **Định dạng:** Apache Parquet (Tối ưu cho truy vấn Big Data)

## 2. Các thành phần chính
- **Cấu trúc lưu trữ:**
  - `s3a://hospital-lakehouse/raw/patients/`
  - `s3a://hospital-lakehouse/raw/encounters/`
  - `s3a://hospital-lakehouse/raw/conditions/`
- **Script xử lý:** `spark_jobs/postgres_to_raw.py`
- **Điều phối:** Airflow DAG `ingest_hospital_data.py` hoặc Docker Service `spark-raw-job`.

## 3. Cách thực hiện

### Cách 1: Chạy thủ công qua Docker Compose (Khuyên dùng khi Dev)
```bash
docker compose --env-file .env -f deploy/docker-compose.yml up spark-raw-job
```

### Cách 2: Chạy tự động qua Airflow
Truy cập Airflow UI tại `http://localhost:8082` và kích hoạt DAG `ingest_hospital_data`.

## 4. Kiểm tra kết quả
Sử dụng công cụ `mc` để kiểm tra file trong thư mục raw:
```bash
docker exec mc mc ls --recursive minio/hospital-lakehouse/raw/
```
Bạn sẽ thấy các file `.parquet` được phân bổ trong các thư mục tương ứng.

---
*Cập nhật: 10/04/2026*
