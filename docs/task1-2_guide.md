# Hướng dẫn Step-by-Step Task 1.2: Thiết lập Database nguồn và Nạp dữ liệu Synthea

Dựa trên việc phân tích file requirements và cấu trúc của dữ liệu CSV thực tế, tôi đã chuẩn bị hướng dẫn chi tiết kèm theo file DDL chính xác để bạn sửa lỗi import vừa rồi.

Lỗi bạn gặp lúc chạy `COPY` (`missing data for column "healthcare_coverage"`) thường do cấu trúc bảng DDL bạn tạo chưa trùng khớp hoàn toàn với cấu trúc thật của file Synthea CSV, hoặc số lượng cột bị sai lệch (có thể do lỗi copy/paste hoặc không khai báo đầy đủ các trường null).

Dưới đây là các bước chuẩn xác nhất:

## Bước 1: Truy cập container và dọn dẹp bảng cũ
Vì bạn đã tạo bảng bị sai Schema, hãy drop database hoặc các bảng cũ:
```bash
docker exec -it postgres-source psql -U admin -d hospital_db
```

Trong psql, xóa các bảng lỗi:
```sql
DROP TABLE IF EXISTS conditions CASCADE;
DROP TABLE IF EXISTS encounters CASCADE;
DROP TABLE IF EXISTS patients CASCADE;
```

## Bước 2: Chạy script DDL (SQL) tạo bảng chính xác

Chạy script sau trong cửa sổ `psql` để tạo các bảng với kiểu dữ liệu đã được map chuẩn theo Synthea (sử dụng UUID, DATE, NUMERIC):

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

> [!NOTE]
> Tư vấn dữ liệu: Sử dụng kiểu **TIMESTAMPTZ** thay cho TIMESTAMP thường để lưu timezone và **NUMERIC** thay cho Float hoặc Money đối với các cột tài chính (`healthcare_expenses`, `cost`...).

## Bước 3: Đẩy file CSV vào PostgreSQL container
Bạn đã làm đúng bước `docker cp` ở lần trước. Chạy các lệnh sau trên **Powershell của Windows (thoát khỏi psql bằng `\q`)**:

```bash
docker cp data/patients.csv postgres-source:/tmp/patients.csv
docker cp data/encounters.csv postgres-source:/tmp/encounters.csv
docker cp data/conditions.csv postgres-source:/tmp/conditions.csv
```

## Bước 4: Nạp dữ liệu (COPY)
Vào lại `psql` bằng lệnh:
```bash
docker exec -it postgres-source psql -U admin -d hospital_db
```

Chạy lệnh COPY (chú ý thứ tự vì khóa ngoại):

```sql
COPY patients FROM '/tmp/patients.csv' DELIMITER ',' CSV HEADER;
COPY encounters FROM '/tmp/encounters.csv' DELIMITER ',' CSV HEADER;
COPY conditions FROM '/tmp/conditions.csv' DELIMITER ',' CSV HEADER;
```

## Bước 5: Kiểm tra số lượng (Verify)
Trong `psql`, chạy các lệnh sau để đảm bảo dữ liệu toàn vẹn:

```sql
SELECT count(*) AS total_patients FROM patients;
SELECT count(*) AS total_encounters FROM encounters;
SELECT count(*) AS total_conditions FROM conditions;
```

> [!TIP]
> Do Synthea chứa rất nhiều bản ghi (Encounter lên tới hơn 3 triệu dòng), quá trình `COPY` có thể mất từ vài giây đến một phút là hoàn toàn bình thường.

## PHẦN MỞ RỘNG: Quy trình thêm bảng dữ liệu mới
Khi dự án tiếp tục mở rộng và bạn cần nạp thêm các file CSV (ví dụ: `immunizations.csv`, `medications.csv`, `observations.csv`), hãy thực hiện quy trình sau để đảm bảo tính toàn vẹn:

1. **Đọc và Phân tích dữ liệu gốc:**
   - Xem cấu trúc file CSV (Dùng Python log 5 dòng đầu hoặc mở Preview file).
   - Xác định chính xác **tên cột, số lượng cột** và **kiểu dữ liệu** của từng trường (ngày tháng, chuỗi, hay số float).
   - Kiểm tra liên kết khóa ngoại (Foreign Keys). Ví dụ: cột `PATIENT` sẽ tham chiếu đến `patients(id)`.

2. **Viết script DDL (SQL):**
   - Định nghĩa DDL với `CREATE TABLE`.
   - Lựa chọn kiểu dữ liệu tối ưu của PostgreSQL (`UUID` cho ID, `DATE` hoặc `TIMESTAMPTZ` cho thời gian, `TEXT` hoặc `VARCHAR` cho chuỗi, `NUMERIC` cho tiền tệ/điểm số).
   - Khai báo rõ ràng Khóa chính (`PRIMARY KEY`) và Khóa ngoại (`REFERENCES`).

3. **Copy File vào trong Container:**
   - Sử dụng Docker CP để đưa file vào Container:
     ```bash
     docker cp data/new_table.csv postgres-source:/tmp/new_table.csv
     ```

4. **Nạp (COPY) và Xác thực (Verify):**
   - Vào lại psql và chạy lệnh nạp dữ liệu:
     ```sql
     COPY new_table FROM '/tmp/new_table.csv' DELIMITER ',' CSV HEADER;
     ```
   - Chạy `SELECT count(*) FROM new_table;` hoặc `SELECT * FROM new_table LIMIT 5;` để đảm bảo dữ liệu ghi thành công và không bị lệch cột.
