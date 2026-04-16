# Development Guide - Healthcare Lakehouse Project

Tài liệu này tổng hợp toàn bộ quy trình thiết lập và vận hành hệ thống Data Lakehouse từ Task 1.2 đến 1.5.

---

## 1. Setup & Khởi tạo Hệ thống nguồn (Task 1.2)

### Bước 1: Dọn dẹp môi trường cũ (Nếu đã từng chạy lỗi)
Để đảm bảo PostgreSQL chạy lại script khởi tạo (`init.sql`), bạn cần xóa container và quan trọng nhất là xóa Volume cũ:
```bash
docker compose -f deploy/docker-compose.yml down -v
```
*(Tham số `-v` sẽ xóa sạch dữ liệu cũ trong Database để nạp mới hoàn toàn).*

### Bước 2: Kiểm tra dữ liệu đầu vào
Đảm bảo các file dữ liệu Synthea (.csv) đã nằm đúng trong thư mục `data/` ở thư mục gốc của dự án:
- `data/patients.csv`
- `data/encounters.csv`
- `data/conditions.csv`

### Bước 3: Khởi chạy Database
Từ thư mục gốc của dự án, chạy lệnh khởi động riêng Database nguồn:
```bash
docker compose --env-file .env -f deploy/docker-compose.yml up -d postgres-source
```

### Bước 4: Theo dõi quá trình nạp dữ liệu (Quan trọng)
Mở terminal khác kiểm tra,
```bash
docker logs postgres-source -f
```
Do file `encounters.csv` rất lớn (~1GB), PostgreSQL sẽ mất khoảng 1-3 phút để thực hiện lệnh `COPY` ngầm. Bạn hãy theo dõi log để biết khi nào hoàn tất:

Khi bạn thấy dòng log: `PostgreSQL init process complete; ready for start up.`, nghĩa là toàn bộ bảng đã được nạp xong.


 khi nào số lượng bảng tăng lên thì dừng lại hoặc chạy lệnh bên dưới bước 5 để kiểm tra mà nó đếm ra cột không phải 0 là được
Lưu ý: nó hiện 0 rows tức chưa chạy xong chứ không phải lỗi

### Bước 5: Kiểm tra Verify dữ liệu
Sau khi nạp xong, hãy chạy lệnh này để xác nhận số dòng trong các bảng:
```bash
# Đếm số lượng bệnh nhân (Kỳ vọng ~124k)
docker exec -it postgres-source psql -U admin -d hospital_db -c "SELECT count(*) FROM patients;"

# Đếm số lượng cuộc gặp (Kỳ vọng ~3.1M)
docker exec -it postgres-source psql -U admin -d hospital_db -c "SELECT count(*) FROM encounters;"

#Đếm khoảng 1 triệu 1
docker exec -it postgres-source psql -U admin -d hospital_db -c "SELECT count(*) FROM conditions;" 
```

---

## 2. Cấu hình Object Storage - MinIO & Buckets (Task 1.3)

Hệ thống đã tự động hóa 90% quá trình này trong file `docker-compose.yml` thông qua container `mc`.

### Bước 1: Khởi động toàn bộ hạ tầng
Chạy lệnh sau để bật các dịch vụ còn lại (MinIO, Metastore, Trino, v.v.):
==> nếu chạy all dịch vụ giống lệnh sau thì khúc task sau chỉ cần test, không cần chạy lại
```bash
docker compose --env-file .env -f deploy/docker-compose.yml up -d
```

### Bước 2: Chờ Container mc tự động cấu hình
Container `mc` (MinIO Client) sẽ tự động chạy ngầm để:
- Tạo bucket `hospital-lakehouse` và `warehouse`.
- Tạo các thư mục: `raw/`, `bronze/`, `silver/`, `gold/`.
- Tạo tài khoản chuyên dụng: `lakehouse_admin` (Password: `Lakehouse123!`).

Kiểm tra trạng thái: `docker logs mc`

### Bước 3: Kiểm tra giao diện MinIO Console (UI)
- **URL:** `http://localhost:9001`
- **Đăng nhập:** Tài khoản Admin trong `.env` (mặc định: `admin` / `password`).
- Xác nhận sự hiện diện của 2 bucket: `hospital-lakehouse` và `warehouse`.

### Bước 4: Kiểm tra tài khoản lakehouse_admin (logout thằng cũ ra trước)
Thử đăng nhập vào UI bằng tài khoản phụ để chắc chắn phân quyền thành công:
- **User:** `lakehouse_admin`
- **Pass:** `Lakehouse123!`

---

## 3. Nạp dữ liệu vào lớp Raw - Parquet (Task 1.4)

Mục tiêu: Chuyển dữ liệu từ Dạng bảng (Postgres) sang Dạng file (Parquet - Raw Layer).

### Bước 1: Khởi chạy Job nạp dữ liệu Raw
```bash
docker compose --env-file .env -f deploy/docker-compose.yml up spark-raw-job
```

### Bước 2: Kiểm tra Log
Theo dõi quá trình trích xuất:
```bash
docker logs spark-raw-job -f
```
Dấu hiệu thành công: `[DONE] Tất cả bảng đã được đẩy lên raw/`.

### Bước 3: Xác minh dữ liệu vật lý
Liệt kê các file đã nạp trên MinIO:
```bash
docker exec mc mc ls --recursive minio/hospital-lakehouse/raw/
```

---

## 4. Nạp dữ liệu vào lớp Bronze - Iceberg (Task 1.5)

Mục tiêu: Chuyển đổi Parquet thô thành bảng Iceberg chuyên nghiệp để quản lý metadata và tăng tốc truy vấn.

### Bước 1: Khởi chạy Job Bronze
```bash
docker compose --env-file .env -f deploy/docker-compose.yml up --force-recreate spark-bronze-job
```

### Bước 2: Theo dõi tiến trình
```bash
docker logs spark-bronze-job -f
```
Dấu hiệu thành công: `🎉 Hoàn tất quá trình Ingestion vào lớp Bronze!`

### Bước 3: Truy vấn kiểm tra dữ liệu với Trino
Vào Trino CLI:
```bash
docker exec -it trino trino
```
Thực hiện SQL:
```sql
SHOW TABLES IN iceberg.bronze;
SELECT count(*) FROM iceberg.bronze.encounters; -- Kỳ vọng: 3,188,675
SELECT * FROM iceberg.bronze.patients LIMIT 5;
```

---

## 5. Quy tắc chung & Bảo trì

- **Biến môi trường:** Luôn sử dụng lệnh kèm `--env-file .env`. Nếu thay đổi thông tin trong `.env`, hãy cập nhật tương ứng các lệnh `docker exec`.
- **Dừng hệ thống:** `docker compose -f deploy/docker-compose.yml down`
- **Lớp tiếp theo:** Sau khi hoàn tất Bronze, dữ liệu đã sẵn sàng để xử lý tại lớp **Silver Layer** (lọc nhiễu, chuẩn hóa kiểu dữ liệu).
