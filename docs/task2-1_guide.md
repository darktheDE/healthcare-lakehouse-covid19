# Task 2.1 Guide: Silver Layer COVID Clinical Master

Tài liệu này tổng hợp đầy đủ quá trình triển khai Silver Layer cho bài toán COVID clinical analytics: chúng ta đã làm gì, vì sao làm như vậy, ý nghĩa của từng phép biến đổi dữ liệu, các lỗi đã gặp, cách xử lý và kết quả đạt được.

## 1. Mục tiêu của Task 2.1
- Xây dựng bảng Silver theo hướng patient-centric để phục vụ phân tích COVID.
- Chuẩn hóa dữ liệu từ Bronze nhằm tăng độ tin cậy cho truy vấn phân tích.
- Giảm chi phí truy vấn lặp lại bằng cách pre-aggregate các chỉ số quan trọng.
- Tạo đầu ra ổn định để sẵn sàng cho lớp Gold, dashboard BI và phân tích nâng cao.

## 2. Bối cảnh và phạm vi
- Input chính: `hospital.bronze.patients`, `hospital.bronze.encounters`, `hospital.bronze.conditions`.
- Input tùy chọn: `hospital.bronze.observations` (nếu có).
- Output Silver: `hospital.silver.covid_clinical_master`.
- Script xử lý: `spark_jobs/silver_cleansing.py`.
- Cách chạy qua Docker Compose: service `spark-silver-job` trong `deploy/docker-compose.yml`.

## 3. Chuẩn bị trước khi thực hiện
1. Hạ tầng Lakehouse đã chạy (`minio`, `hive-metastore`, `spark-iceberg`, `trino`, `postgres-source`).
2. Dữ liệu Raw và Bronze đã được nạp thành công (Task 1.4, 1.5).
3. Có file `.env` hợp lệ chứa key truy cập MinIO và DB.
4. Đảm bảo Trino có thể truy cập catalog Iceberg để verify output.

## 4. Những gì đã làm và vì sao làm như vậy

### 4.1 Chuẩn hóa dữ liệu đầu vào (Data Cleansing)
Chúng ta làm sạch dữ liệu trước khi join/aggregate:
- Chuẩn hóa định danh (`patient_id`, `encounter_id`) bằng trim + null handling.
- Ép kiểu thời gian/ngày (`to_date`, `to_timestamp`) để đảm bảo nhất quán semantic.
- Chuẩn hóa giới tính về dạng chuẩn (`Male`, `Female`, hoặc giá trị đã chuẩn hóa).
- Loại bỏ bản ghi không đủ khóa join và xử lý trùng lặp theo khóa nghiệp vụ.

Ý nghĩa:
- Tránh sai số khi join do mismatch kiểu hoặc định danh rỗng.
- Tránh đếm trùng ở các phép tổng hợp.
- Đảm bảo dữ liệu đầu vào có chất lượng đủ tốt cho phân tích cohort.

### 4.2 Xây cohort COVID theo tiêu chí nghiệp vụ
Cohort COVID được xác định từ:
- Conditions code `840539006`.
- Hoặc observations có tín hiệu dương tính (`POSITIVE|DETECTED|REACTIVE`) khi bảng observations tồn tại.

Ý nghĩa:
- Gom đúng tập bệnh nhân mục tiêu cho phân tích lâm sàng COVID.
- Hỗ trợ linh hoạt: nếu thiếu observations vẫn chạy được theo điều kiện conditions.

### 4.3 Tổng hợp theo bệnh nhân (Patient-centric aggregation)
Sau khi xác định cohort, dữ liệu được tổng hợp theo `patient_id`:
- `encounter_count`, `last_encounter_time`, `total_claim_cost_sum`.
- `condition_count`, `last_condition_date`.
- `observation_count`, `last_observation_time` (nếu có observations).

Ý nghĩa:
- Chuyển dữ liệu event-level sang entity-level để phân tích nhanh hơn.
- Hỗ trợ use case phân tầng nguy cơ, phân tích tần suất chăm sóc và chi phí.

### 4.4 Tối ưu hiệu năng để xử lý lỗi OOM
Trong quá trình chạy thực tế, chúng ta gặp OOM ở bước aggregate/write. Các tối ưu đã áp dụng:
- Cohort-first strategy: chỉ aggregate trên tập COVID thay vì toàn bộ dataset.
- `repartition("patient_id")` trước `groupBy` để phân phối dữ liệu đều hơn.
- Tránh phép tính nặng không cần thiết trong bối cảnh này.
- Tăng tài nguyên và tuning Spark submit:
  - `--driver-memory 3g`
  - `spark.sql.adaptive.enabled=true`
  - `spark.sql.adaptive.coalescePartitions.enabled=true`
  - `spark.sql.shuffle.partitions=256`

Ý nghĩa:
- Giảm shuffle pressure và giảm xác suất tràn bộ nhớ.
- Tăng độ ổn định của pipeline khi dữ liệu lớn.

## 5. Vấn đề gặp phải và hướng giải quyết

### Vấn đề 1: Thiếu observations ở Bronze
- Triệu chứng: bảng observations không tồn tại trong một số lần chạy.
- Giải pháp: thiết kế pipeline tolerant với optional input.
- Kết quả: job vẫn chạy thành công bằng nhánh logic conditions-only.

### Vấn đề 2: Lỗi `java.lang.OutOfMemoryError: Java heap space`
- Triệu chứng: fail tại giai đoạn aggregate/write của Silver.
- Nguyên nhân chính: khối lượng shuffle/aggregation lớn trên toàn tập dữ liệu.
- Giải pháp:
  - Thu hẹp dữ liệu trước khi aggregate (cohort-first).
  - Repartition theo khóa nhóm.
  - Bật adaptive execution và tăng driver memory.
- Kết quả: job chạy hoàn tất, ghi bảng Iceberg thành công.

## 6. Step-by-step thực hiện Task 2.1

### Bước 1: Đảm bảo nền tảng chạy ổn định
```bash
docker compose --env-file .env -f deploy/docker-compose.yml up -d
```

### Bước 2: Đảm bảo Bronze đã sẵn sàng
```bash
docker compose --env-file .env -f deploy/docker-compose.yml up spark-raw-job
docker compose --env-file .env -f deploy/docker-compose.yml up spark-bronze-job
```

### Bước 3: Chạy Silver job
```bash
docker compose --env-file .env -f deploy/docker-compose.yml run --rm spark-silver-job
```

### Bước 4: Verify qua Spark log
Các tín hiệu mong đợi trong log:
- `COVID clinical master rows: <n>`
- `Writing target table: hospital.silver.covid_clinical_master`
- `Persisted row count: <n>`
- `Silver table enrichment completed successfully`

### Bước 5: Verify bằng Trino
Truy cập vào Trino CLI bằng `docker exec -it trino trino`
```sql
SHOW TABLES IN iceberg.silver;
SELECT count(*) FROM iceberg.silver.covid_clinical_master;
```

### Bước 6: Verify vật lý trên MinIO (Iceberg files)
```bash
docker exec mc mc ls --recursive minio/hospital-lakehouse/silver.db/covid_clinical_master/
```
Kỳ vọng có cả `data/*.parquet` và `metadata/*` của Iceberg.

## 7. Output đạt được và mức độ sẵn sàng

### Output chính
- Bảng Silver: `hospital.silver.covid_clinical_master`.
- Số dòng persisted nhất quán với số dòng tính toán trong job.
- Metadata Iceberg đầy đủ trong MinIO.

### Output này đã sẵn sàng cho việc gì
1. Làm nguồn cho Gold layer (KPI tổng hợp theo thời gian/nhóm bệnh nhân).
2. Kết nối BI (Trino -> Superset/Metabase/Tableau) để dựng dashboard.
3. Phân tích chi phí điều trị theo cohort COVID.
4. Phân tích tần suất encounter/condition theo bệnh nhân.
5. Chuẩn bị feature đầu vào cho mô hình dự báo nguy cơ hoặc mức độ sử dụng dịch vụ y tế.

## 8. Ý nghĩa phân tích dữ liệu của các phép biến đổi
- Identifier normalization: đảm bảo liên kết entity nhất quán, giảm lỗi sai join.
- Date/time casting: mở khóa phân tích theo timeline (trend, seasonality, recency).
- Deduplication: nâng độ tin cậy của metric (đếm đúng số lượt/sự kiện).
- Cohort extraction: tập trung đúng nhóm nghiên cứu, tránh nhiễu từ non-COVID.
- Patient-level aggregation: biến dữ liệu vận hành thành dữ liệu phân tích.
- Cost aggregation: hỗ trợ phân tích burden tài chính và tối ưu chăm sóc.
- Optional observations path: pipeline resilient với dữ liệu thiếu/không đồng đều.
- Iceberg write: hỗ trợ ACID + schema evolution + query performance cho các lớp sau.

## 9. Kết luận
Task 2.1 đã chuyển thành công dữ liệu Bronze thành một Silver dataset có giá trị phân tích cao, ổn định vận hành và sẵn sàng cho các tác vụ phân tích nâng cao ở Gold/BI. Những tối ưu ở bước aggregate đã giải quyết trực tiếp bottleneck OOM, đồng thời giữ nguyên tính đúng đắn nghiệp vụ của cohort COVID.
