# Development Guide

## 0. Link datasource: https://mitre.box.com/shared/static/wk3560f962ozlg7sd2oj1zxk73ayqvm0.zip
==> down về bỏ vào folder /data

## 1. Setup & Chạy dự án

1. Clone repository về máy:
   ```bash
   git clone https://github.com/darktheDE/healthcare-lakehouse-covid19.git
   cd healthcare-lakehouse-covid19
   ```
2. Cấu hình biến môi trường và chạy hệ thống:
   Đảm bảo bạn có file `.env` (chứa tài khoản MinIO, Database) nằm ở thư mục gốc của project (ngang hàng `deploy/`).
   ```bash
   # Bước 1: Pull code mới nhất
   git checkout develop
   git pull origin develop
   
    # Bước 2: Start nền tảng Lakehouse với .env (Gồm MinIO, Metastore, Trino, Airflow)
    docker-compose --env-file .env -f deploy/docker-compose.yml up -d
    ```
3. Chạy các tiến trình nạp dữ liệu (Ingestion):
   ```bash
   # Chạy nạp dữ liệu từ Postgres vào Raw (Parquet)
   docker-compose --env-file .env -f deploy/docker-compose.yml up spark-raw-job

   # Chạy nạp dữ liệu từ Raw vào Bronze (Iceberg)
   docker-compose --env-file .env -f deploy/docker-compose.yml up spark-bronze-job
   ```
4. Truy cập các công cụ:
   - **MinIO Console:** `http://localhost:9001` (User: admin)
   - **Airflow UI:** `http://localhost:8082` (standalone mode)
   - **Trino CLI:** `docker exec -it trino trino`
5. Dừng các dịch vụ:
   ```bash
   docker-compose -f deploy/docker-compose.yml down
   ```

## 2. Quy trình phát triển (Agile/Scrum với Plane.so)

**Chuẩn bị Task:**
1. Lên Plane.so chọn task cần làm.
2. Đọc tài liệu lệnh quan (`DEVELOPMENT.md`, yêu cầu của task).
3. Assign task cho chính mình (Assignee).
4. Assign người Reviewer.
5. Đặt Deadline hoàn thành code cho người làm (đến khi có PR).
6. Chuyển trạng thái task từ `Todo` sang `In Progress`.

**Tiến hành Code:**
1. Cập nhật nhánh `develop` mới nhất:
   ```bash
   git checkout develop
   git pull origin develop
   ```
2. Tạo nhánh từ `develop` theo format `feature/<module>/<mã-task-trên-plane>`:
   ```bash
   git checkout -b feature/<module>/<mã-task>
   ```
3. *(Khuyến khích)* Tạo file `<mã-task>.md` để lưu lại quá trình phát triển (cách đã làm, lỗi gặp phải, cách fix) làm tài liệu lưu trữ sau này.
4. Thực hiện code và commit:
   ```bash
   git add .
   git commit -m "feat: mô tả ngắn gọn sửa đổi"
   ```
5. Đẩy code lên repository:
   ```bash
   git push origin feature/<module>/<mã-task>
   ```

**Pull Request (PR) & Code Review:**
1. Tạo PR từ nhánh `feature/...` vào nhánh `develop`.
2. Đặt Deadline cho Reviewer (thời gian tối đa để kiểm tra và Accept PR).
3. Reviewer kiểm tra code và duyệt qua.
4. Quản lý repo tiến hành Merge PR khi hoàn tất review 3 bên.

**Lịch Họp:**
* Họp nhóm 2 ngày/buổi, vào **22h30 mỗi tối**.

## 3. Quy tắc đặt tên

### Nhánh (Branch)
* `feature/<module>/<task>` - Tính năng / công việc mới
* `bugfix/...` - Sửa lỗi
* `chore/...` - Cập nhật thư viện, cấu hình

### Commit
* `feat:` - Tính năng mới
* `fix:` - Sửa lỗi
* `docs:` - Tài liệu
* `refactor:` - Cải thiện / tối ưu code
* `chore:` - Tooling, setup no production code change

---

## 4. Tổng quan Hệ thống Tự Động (Kiến trúc Medallion Lakehouse)

* **Processing (Apache Iceberg & Spark):** Hệ thống không còn dùng file CSV trung gian kém hiệu quả. Dữ liệu từ Postgres được Spark "nuốt" trực tiếp qua JDBC thành định dạng **Parquet** (Raw Layer), sau đó được chuyển đổi chuẩn thành **Table Iceberg** (Bronze Layer) thông qua các job container tự động.
* **SQL Engine (Trino):** Cung cấp khả năng truy vấn dữ liệu Big Data tức thì trên lớp Bronze, Silver, Gold thông qua cổng 8081. Đã được cấu hình chuẩn `fs.native-s3` cho Trino 480+.

## 5. Hướng Dẫn Tác Chiến Của Maintainer Tương Lai

**Trạng thái tính đến nay (Task 1.2 -> 1.5):** 
Hạ tầng Core Lakehouse đã quá xuất sắc. Tệp cấu hình cực sạch ở `.env`, không lộ Secret, dòng chảy **Postgres -> Parquet (Raw) -> Iceberg (Bronze)** đã tự động hóa 100%. Đã kiểm chứng 4.4 triệu bản ghi sẵn sàng để xử lý tiếp.

**Việc tiếp theo của các Maintainers:**
1. Đọc và lấy cảm hứng kĩ thuật để phân tích kiến trúc qua `docs/project_architecture_roadmap.md` và `docs/task..._guide.md`.
2. **Triển Khai Mạng Lưới Silver Layer:** Thiết lập xử lý Spark lọc Nhiễu, xử lý Cột Trống, ép kiểu tiền tệ sang `DECIMAL(18, 2)`, thực hiện phân vùng (Partitioning) theo `event_month` trong Iceberg nhằm tăng tốc truy vấn.
3. **Orchestration Airflow:** Trưởng thành hóa dòng chảy BATCH PIPELINE bằng việc kích hoạt các **Apache Airflow DAG** để điều phối toàn bộ job Spark theo chu kỳ tự động.
4. **Data Visualization:** Cắm các công cụ BI (Superset, Metabase, Tableau) vào Trino để báo cáo dữ liệu.
