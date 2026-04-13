# Task 2.0:[Spark-Iceberg] Lớp Gold - Phân tích Triệu chứng và Biến chứng lâm sàng

* **Mô tả:**\
  Tham chiếu trang 2 và 8 của file PDF (Synthea COVID-19 Analysis). Task này sử dụng Spark để thống kê tỷ lệ các biến chứng (Sepsis, ARDS, Heart Failure) và triệu chứng (Cough, Fever) xuất hiện _sau_ khi bệnh nhân nhiễm COVID-19, đồng thời so sánh tỷ lệ này giữa nhóm Sống (Survivors) và Tử vong (Non-survivors).

* **Step-by-Step (Bản gốc AI Studio):**

  1. **Đọc dữ liệu:** Load bảng hospital.silver.covid_clinical_master (đã join sẵn patients, encounters, conditions).
  2. **Logic Thời gian (Time-series filter):** Lọc ra các conditions có ngày bắt đầu (START) lớn hơn hoặc bằng ngày bắt đầu chẩn đoán COVID-19 của chính bệnh nhân đó.
  3. **Dán nhãn (Labeling):** Tạo cột is_survivor (True nếu DEATHDATE là null, False nếu có ngày chết).
  4. **Aggregation:** Thực hiện groupBy('Condition_Description', 'is_survivor').
  5. **Tính toán:** Dùng hàm count() để đếm số lượng bệnh nhân mắc từng triệu chứng/biến chứng. Tính tỷ lệ % trên tổng số bệnh nhân cùng nhóm.
  6. **Ghi Iceberg:** Ghi DataFrame kết quả vào bảng [hospital.gold](http://hospital.gold).symptoms_outcomes.

---

## KIỂM TRA NGỮ CẢNH VÀ KẾ HOẠCH TRIỂN KHAI (CẬP NHẬT CHUẨN MEDALLION)

**1. Đánh giá tính hợp lý (Context Check & Architecture Validation):**
* Bản hướng dẫn gốc mặc định `hospital.silver.covid_clinical_master` chứa thông tin *event-level*. Tuy nhiên, script Task 2.1 tạo ra bảng này là dạng **patient-centric** tổng hợp, không có danh sách biến chứng.
* Ban đầu tôi đề xuất query trực tiếp với `hospital.bronze.conditions`. Tuy nhiên, đánh giá lại theo chuẩn **Medallion Architecture**, việc Gold đọc thẳng từ Bronze là **không hợp lý và phá vỡ quy tắc thiết kế** (vì Bronze chưa được clean, ép kiểu, lọc null).
* **Đề xuất thay đổi chuẩn (RẤT CẦN THIẾT):** 
  - Đẩy dữ liệu conditions lên thành một bảng Silver độc lập `hospital.silver.conditions`.
  - Lớp Gold sẽ chỉ gọi từ các bảng Silver (`silver.covid_clinical_master` và `silver.conditions`) để thực thi Data Logic.

**2. Kế hoạch triển khai (Step-by-Step cập nhật):**

**Phase 1: Bổ sung Output Lớp Bạc (Silver Layer Update)**
1. Sửa file `spark_jobs/silver_cleansing.py`.
2. Hàm `clean_conditions()` đã thực thi việc format date, clean null, deduplicate tốt. Chúng ta chỉ cần thêm lệnh write DataFrame `conditions_clean` ra bảng Iceberg `hospital.silver.conditions`.

**Phase 2: Triển khai Lớp Vàng (Gold Layer Analytics)**
1. **Đọc dữ liệu:** Load 2 bảng chuẩn: `hospital.silver.covid_clinical_master` và `hospital.silver.conditions`.
2. **Tìm Ngày nhiễm COVID (Diagnosis Date):** Lọc bảng `silver.conditions` với `condition_code = '840539006'`, groupby `patient_id` để lấy `min(condition_start_date)` làm ngày bắt đầu nhiễm.
3. **Lọc Triệu chứng & Biến chứng:** Join tập điều kiện với ngày nhiễm. Lọc các `condition_start_date >= diagnosis_date` và `condition_description` khớp với (Sepsis, ARDS, Heart Failure, Cough, Fever).
4. **Dán nhãn Survivor:** Join với `silver.covid_clinical_master` để lấy `patient_deathdate`. Tạo cột `is_survivor = (patient_deathdate is null)`.
5. **Tổng hợp Metric (Numerator):** Dùng `groupBy('condition_description', 'is_survivor')` và `countDistinct(patient_id)`.
6. **Tổng hợp Population (Denominator):** Đếm tổng lượng bệnh nhân rẽ nhánh Survivor/Non-survivor từ bảng Silver master.
7. **Tính Phần trăm:** Trộn dữ liệu và tính `%_rate = (Numerator / Denominator) * 100`. Làm tròn 2 chữ số.
8. **Ghi Iceberg:** Ghi đè vào `hospital.gold.symptoms_outcomes`.
9. **Cập nhật script:** Viết toàn bộ Phase 2 vào file `spark_jobs/gold_aggregation.py`.

Kế hoạch này đảm bảo tính bền vững của luồng Datalake, loại bỏ mã lặp (như việc clean nhiều lần), và đáp ứng hoàn hảo yêu cầu phân tích kinh doanh. Chờ duyệt để bắt tay vào code!

---

## NHẬT KÝ LẬP TRÌNH VÀ TRIỂN KHAI (IMPLEMENTATION LOG)

Sau khi kế hoạch được duyệt, quá trình code và tích hợp đã được thực thi như sau:

**1. Sửa file `spark_jobs/silver_cleansing.py` (Phase 1):**
- Đã thêm đoạn code để ghi `conditions_clean` DataFrame thành 1 bảng độc lập trên Lakehouse.
```python
    CONDITIONS_OUTPUT_TABLE = f"{SILVER_NAMESPACE}.conditions"
    print(f"[INFO] Writing target table: {CONDITIONS_OUTPUT_TABLE}")
    conditions_clean.writeTo(CONDITIONS_OUTPUT_TABLE).createOrReplace()
```

**2. Khởi tạo `spark_jobs/gold_aggregation.py` (Phase 2):**
- Đã dựng toàn bộ pipeline ETL Layer Gold bằng PySpark.
- Khởi tạo Spark Session với config Hive Metastore/S3/MinIO.
- Kết nối `hospital.silver.covid_clinical_master` => Extract Cohort, tính Base Population (Denominator) cho từng nhánh Survivor.
- Kết nối `hospital.silver.conditions`, filter những records (1) Thuộc nhóm Keywords mục tiêu và (2) Xảy ra **cùng ngày hoặc sau ngày nhận xét COVID** (mã `840539006`).
- Group by từng Symptom/Complication và Survivor status, tính Count_Distinct `patient_id`.
- Ráp vào Denominator và tính `% rate` (Round 2 chữ số).
- Xuất result table ra bảng `hospital.gold.symptoms_outcomes`.

**3. Tích hợp Container `spark-gold-job`:**
- Ở file `deploy/docker-compose.yml`, tôi đã tạo mới service container `spark-gold-job` tái sử dụng config của môi trường Spark-Iceberg.
- Lệnh Submit script: `spark-submit ... /opt/spark_jobs/gold_aggregation.py`
- Tự động chạy tuần tự: Dependency được gắn nối tiếp với `spark-silver-job`.

**4. Kết quả & Đóng gói:**
Bây giờ, pipeline Lakehouse đã có khả năng chạy xuyên suốt (End-to-End):
```bash
docker compose --env-file .env -f deploy/docker-compose.yml up spark-silver-job spark-gold-job
```
* Dữ liệu Gold sẵn sàng để cung cấp cho `Trino` và `Superset` trực quan hóa! All processes completed.
