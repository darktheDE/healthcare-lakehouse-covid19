# Hướng dẫn Task 1.3: Cấu hình Object Storage (MinIO) và Buckets

Tài liệu này trình bày cả hai phương pháp để thiết lập Storage Buckets cho kiến trúc Medallion: (1) Cách thức sử dụng bằng tay giúp bạn biết thao tác UI cơ bản và (2) Cách thiết lập tự động hóa qua Docker cho đội nhóm.

---

## PHẦN 1: CÁC BƯỚC THIẾT LẬP THỦ CÔNG (MANUAL)

### Bước 1: Đăng nhập MinIO Console
1. Truy cập trình duyệt để tới MinIO GUI: `http://localhost:9001`.
2. Đăng nhập bằng Account Admin (Theo `docker-compose` là `admin`/`password`).

### Bước 2: Tạo Bucket và Phân vùng
1. Bấm **Create Bucket**, nhập tên là `hospital-lakehouse` rồi khởi tạo.
2. Tại Object Browser, điều hướng vào `hospital-lakehouse`. Nhấn nút **Create new path** để tạo lần lượt các folders (prefix) ảo: `raw/`, `bronze/`, `silver/`, `gold/`.
*Lưu ý:* Khi Prefix của MinIO chưa chứa tệp nào, nó có thể bị UI tự động ẩn đi. Bạn có thể up 1 tệp `readme.txt` giữ chỗ (dummy) vào trong thư mục đó để chúng hiện rõ.

### Bước 3: Sinh Access Key từ công cụ dòng lệnh 
*(Ghi chú: MinIO Community Edition thường ẩn giao diện Settings Access Key. Ta cần sinh Key qua công cụ dòng lệnh được tích hợp).*
Mở Powershell trên máy (Windows host) và chạy trình tự (Thay `admin` `password` bằng giá trị trong `.env` nếu bạn có thay đổi):
```bash
docker exec minio mc alias set minio http://localhost:9000 admin password
docker exec minio mc admin user svcacct add minio admin
```
Hệ thống sẽ nhả file trên terminal, ví dụ:
```text
Access Key: Z6CDR73ZT2UFJH1QK140
Secret Key: +tbk+aM+IwoFUTHauSCZ8LWVqSjoJ1q2z9BOeH+f
```
*(Ghi nhớ cặp Random Key này để kết nối tới Airflow/Spark).*

---

## PHẦN 2: TỰ ĐỘNG HÓA VỚI DOCKER COMPOSE

Môi trường `docker-compose.yml` của dự án đã được tuỳ biến Container `mc` (nạp bash script `entrypoint`) để tự động gánh thay toàn bộ Phần 1 qua lệnh.

### Bước 1: Chỉ cần gọi Startup
Khi bật hệ thống lên bằng lệnh:
```bash
docker compose --env-file .env -f deploy/docker-compose.yml up -d
```
Container `mc` đính kèm sẽ tự thực thi:
1. Kết nối vào backend `minio:9000`.
2. Tự động sinh Bucket: `hospital-lakehouse`.
3. Tái tạo 4 folders rỗng `raw`, `bronze`, `silver`, `gold` thông qua lệnh `.keep`.
4. Tạo tài khoản cứng/tĩnh: user `lakehouse_admin` chuyên dụng.

### Bước 2: Cấp thông tin cho Data Engineer
Khác với Phần 1 sinh ra random Random Key, luồng setup Tự động này hỗ trợ bạn chốt luôn một key cứng duy nhất để đưa qua `Airflow` hay `Spark` nhằm đảm bảo tính ổn định codebase. Bạn gửi thông tin dưới đây cho đồng sự (Lead DE):

- **S3 Endpoint:** `http://minio:9000`
- **S3 Bucket Name:** `hospital-lakehouse`
- **AWS_ACCESS_KEY_ID:** `lakehouse_admin`
- **AWS_SECRET_ACCESS_KEY:** `Lakehouse123!`
- **Region:** `us-east-1`

### Mở rộng quy mô (Scale-up)
Trong quá trình vận hành Medallion Architecture, nếu muốn tự động hóa thêm Bucket hay Service Account khác:
1. Mở file `deploy/docker-compose.yml`. Tìm khối container `mc`.
2. Ở `entrypoint`, thêm code ví dụ: `/usr/bin/mc mb -p minio/another-bucket;`.
3. Hoặc thêm lệnh `/usr/bin/mc admin user add minio <user_moi> <pass_moi>;` để cấp User Account mới phân quyền nhỏ hơn cho các bộ phận Analytics/BI.
4. Chạy `docker-compose up -d --force-recreate mc` để server tái khởi động lại lệnh bash này.
