# Hướng dẫn Task 1.2: Thiết lập Database nguồn và Nạp dữ liệu Synthea

Lỗi bạn gặp lúc chạy `COPY` (`missing data for column "healthcare_coverage"`) thường do cấu trúc bảng DDL bạn tạo chưa trùng khớp hoàn toàn với cấu trúc thật của file Synthea CSV. Dưới đây là bộ code DDL chuẩn ứng với dữ liệu hiện tại.

Bạn có thể làm theo cách **Thủ công (Phần 1)** để hiểu rõ bản chất hoặc cách **Tự động (Phần 2)** nếu muốn tiết kiệm thời gian khởi tạo.

---

## PHẦN 1: CÁC BƯỚC LÀM THỦ CÔNG (MANUAL)

### Bước 1: Dọn dẹp bảng cũ
```bash
# Kiểm tra .env để lấy DB_SOURCE_USER và DB_SOURCE_NAME
docker exec -it postgres-source psql -U admin -d hospital_db
```
Trong `psql`, xóa các bảng lỗi:
```sql
DROP TABLE IF EXISTS conditions CASCADE;
DROP TABLE IF EXISTS encounters CASCADE;
DROP TABLE IF EXISTS patients CASCADE;
```

### Bước 2: Chạy script DDL tạo bảng chuẩn
Chạy script sau trong cửa sổ `psql` để tạo các bảng với kiểu dữ liệu chuẩn (`UUID`, `DATE`, `NUMERIC`):

```sql
-- Tạo bảng patients
CREATE TABLE patients (
    id UUID PRIMARY KEY,
    birthdate DATE NOT NULL,
    deathdate DATE,
    ssn VARCHAR(20),
    drivers VARCHAR(20),
    passport VARCHAR(20),
    prefix VARCHAR(20),
    first VARCHAR(100),
    last VARCHAR(100),
    suffix VARCHAR(20),
    maiden VARCHAR(100),
    marital VARCHAR(1),
    race VARCHAR(50),
    ethnicity VARCHAR(50),
    gender VARCHAR(1),
    birthplace VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    county VARCHAR(100),
    zip VARCHAR(20),
    lat NUMERIC(10, 6),
    lon NUMERIC(10, 6),
    healthcare_expenses NUMERIC(12, 2),
    healthcare_coverage NUMERIC(12, 2)
);

-- Tạo bảng encounters
CREATE TABLE encounters (
    id UUID PRIMARY KEY,
    start_time TIMESTAMPTZ NOT NULL,
    stop_time TIMESTAMPTZ,
    patient UUID REFERENCES patients(id),
    organization UUID,
    provider UUID,
    payer UUID,
    encounterclass VARCHAR(50),
    code VARCHAR(50),
    description VARCHAR(255),
    base_encounter_cost NUMERIC(12, 2),
    total_claim_cost NUMERIC(12, 2),
    payer_coverage NUMERIC(12, 2),
    reasoncode VARCHAR(50),
    reasondescription VARCHAR(255)
);

-- Tạo bảng conditions
CREATE TABLE conditions (
    start_date DATE NOT NULL,
    stop_date DATE,
    patient UUID REFERENCES patients(id),
    encounter UUID REFERENCES encounters(id),
    code VARCHAR(50),
    description VARCHAR(255)
);
```

### Bước 3: Đẩy file CSV vào PostgreSQL container
Mở Powershell mới chạy lệnh copy từ máy tính vào Container:
```bash
docker cp data/patients.csv postgres-source:/tmp/patients.csv
docker cp data/encounters.csv postgres-source:/tmp/encounters.csv
docker cp data/conditions.csv postgres-source:/tmp/conditions.csv
```

### Bước 4: Nạp dữ liệu (COPY)
Tiếp tục trong `psql`, chạy lệnh lệnh nạp theo trình tự khóa ngoại:
```sql
COPY patients FROM '/tmp/patients.csv' DELIMITER ',' CSV HEADER;
COPY encounters FROM '/tmp/encounters.csv' DELIMITER ',' CSV HEADER;
COPY conditions FROM '/tmp/conditions.csv' DELIMITER ',' CSV HEADER;
```

---

## PHẦN 2: TỰ ĐỘNG HÓA VỚI DOCKER COMPOSE

Hệ thống đã được ánh xạ tự động Script thông qua volume. Bạn không cần gõ hay chép code SQL nữa.

### Bước 1: Khởi chạy Database
Từ thư mục dự án mở Powershell và chạy:
```bash
# Luôn chạy từ thư mục gốc của project để nạp đúng .env
docker compose --env-file .env -f deploy/docker-compose.yml up -d
```

### Bước 2: Chờ quá trình `COPY` ngầm hoàn tất
Do dung lượng bảng `encounters.csv` rất nặng (1GB - 3 triệu dòng), PostgreSQL sẽ từ chối trả về kết quả truy vấn `SELECT` nếu data lúc đó vẫn còn đang chép vào dở. Chờ từ 1-2 phút hoặc mở Logs để theo dõi tiến độ:
```bash
docker logs postgres-source -f
```
Khi thấy dòng log ghi `COPY 3188675`, nghĩa là bảng nặng nhất đã xử lý xong.

### Bước 3: Kiểm tra và Verify (áp dụng cho cả Phần 1 và 2)
Sau khi load thành công, kiểm tra lại dữ liệu toàn vẹn:
```bash
# Sử dụng admin/hospital_db (hoặc giá trị trong .env)
docker exec -it postgres-source psql -U admin -d hospital_db -c "SELECT count(*) AS total_patients FROM patients;"
docker exec -it postgres-source psql -U admin -d hospital_db -c "SELECT count(*) AS total_encounters FROM encounters;"
docker exec -it postgres-source psql -U admin -d hospital_db -c "SELECT count(*) AS total_conditions FROM conditions;"
```

### Mở rộng tự động với bảng mới
Để nạp thêm tự động các file mới như `immunizations.csv`: 
1. Đặt file vào folder `data/` ngoài host.
2. Thêm hàm `CREATE` và `COPY` xuống dưới đáy file `deploy/postgres_init.sql`.
3. Clear DB `docker-compose down -v` và chạy lại.
