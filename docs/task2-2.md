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

# Task 2.2:[Spark-Iceberg] Lớp Gold - Phân tích Tỷ lệ tử vong theo Nhân khẩu học

* **Mô tả:**\
  Tham chiếu trang 11 của file PDF. Tính toán và phân nhóm số lượng ca tử vong do COVID-19 theo từng thập kỷ tuổi (0-10, 10-20, v.v.) và theo Giới tính để phục vụ vẽ biểu đồ Bar Chart.

* **Step-by-Step:**

  1. **Đọc dữ liệu:** Load bảng hospital.silver.covid_clinical_master.

  2. **Lọc dữ liệu:** Chỉ lấy những bệnh nhân đã tử vong (DEATHDATE is not null) và mắc COVID-19.

  3. **Tính Tuổi (Age Calculation):** Dùng hàm Spark SQL datediff() hoặc year() để tính: Tuổi = year(DEATHDATE) - year(BIRTHDATE).

  4. **Phân nhóm Tuổi (Age Binning):** Dùng câu lệnh when().otherwise() trong PySpark để tạo cột mới Age_Range (Ví dụ: when(age <= 10, "0-10").when(age <= 20, "11-20")...).

  5. **Aggregation:** Thực hiện groupBy('Age_Range', 'GENDER'). Dùng hàm count(PATIENT_ID) để ra số ca tử vong.

  6. **Ghi Iceberg:** Lưu kết quả (Mode Overwrite) vào bảng [hospital.gold](http://hospital.gold).mortality_demographics.

---

## KIỂM TRA NGỮ CẢNH VÀ KẾ HOẠCH TRIỂN KHAI TASK 2.2 (CẬP NHẬT CHUẨN MEDALLION)

**1. Đánh giá tính hợp lý (Context Check & Architecture Validation):**
* Bảng `hospital.silver.covid_clinical_master` đã được lọc dành riêng cho bệnh nhân nhiễm COVID-19 trong bước Silver (chỉ chứa COVID cohort). Do đó, chúng ta không cần thêm điều kiện lọc "mắc COVID-19" nữa, mà chỉ cần lọc `patient_deathdate is not null`.
* Ghi dữ liệu vào bảng `hospital.gold.mortality_demographics` là hoàn toàn hợp lý.
* Chúng ta sẽ gộp luôn phần xử lý và lưu bảng này vào chung file `spark_jobs/gold_aggregation.py` (sau tiến trình chạy Symptoms & Outcomes) để tiện quản lý luồng xử lý Lớp Gold.

**2. Kế hoạch triển khai (Step-by-Step cập nhật):**

**Phase 1: Chuẩn bị Script Tính toán**
1. **Đọc dữ liệu:** Load bảng `covid_master_df` (đã có sẵn trong `gold_aggregation.py` - chỉ load một lần).
2. **Lọc dữ liệu:** Lấy những bệnh nhân đã tử vong (`F.col("patient_deathdate").isNotNull()`).
3. **Tính Tuổi (Age Calculation):** Tính tuổi tại thời điểm mất bằng cách `F.year("patient_deathdate") - F.year("patient_birthdate")`.
4. **Phân nhóm Tuổi (Age Binning):** Sử dụng `F.when().otherwise()` để tạo cột `Age_Range` với các mốc: `0-10`, `11-20`, `21-30`, `31-40`, `41-50`, `51-60`, `61-70`, `71-80`, `80+`. Nếu thiếu ngày sinh thì gán `Unknown`.
5. **Aggregation:** Thực hiện `.groupBy('Age_Range', 'gender')`. Đếm số lượng ca tử vong thông qua `F.count("patient_id").alias("mortality_count")`. Cột giới tính sử dụng `gender`.
6. **Ghi Iceberg:** Ghi xuất toàn bộ DataFrame vào bảng `hospital.gold.mortality_demographics` thay thế (Overwrite).

**Phase 2: Chỉnh sửa Script `gold_aggregation.py`**
1. Bổ sung biến kết xuất bảng `MORTALITY_TABLE = f"{GOLD_NAMESPACE}.mortality_demographics"`.
2. Viết thêm khối mã phía dưới logic tính "Symptoms Outcomes" trong hàm `main()` để thực thi tính quy trình trên.
3. Chạy lưu ra bảng Iceberg và kiểm tra tính hợp lệ siêu dữ liệu (metadata checks).

Kế hoạch này đảm bảo tối ưu việc tái sử dụng Spark DataFrame và tạo cơ sở chính xác để vẽ biểu đồ theo yêu cầu. Chờ duyệt để bắt tay vào code!

---

## 3. QUÁ TRÌNH PHÁT TRIỂN VÀ TRIỂN KHAI THỰC TẾ

Dựa trên bản kế hoạch đã được phê duyệt, dưới đây là các bước đã thực hiện để hoàn chỉnh mã nguồn Task 2.2:

1. **Khởi tạo Artifact Theo Dõi:** 
   * Tạo ra `task.md` để mapping các đầu việc chi tiết đảm bảo không bị thiếu sót trong quá trình code logic thống kê.
   
2. **Triển khai Mã nguồn vào `spark_jobs/gold_aggregation.py`:**
   * **Định nghĩa đích đến:** Thêm hằng số `MORTALITY_TABLE = f"{GOLD_NAMESPACE}.mortality_demographics"` ở phần đầu file.
   * **Lọc Dataset:** Do bảng `covid_clinical_master` đã chỉ chứa tập bệnh nhân bị COVID-19, ta trích xuất `mortality_cohort` bằng cách chỉ giữ lại các bản ghi có `patient_deathdate.isNotNull()`.
   * **Transformation (Tính Tuổi):** Thêm cột `age_at_death` sử dụng hàm PySpark `F.year("patient_deathdate") - F.year("patient_birthdate")`.
   * **Transformation (Phân Group tuổi):** Sử dụng liên hoàn các mệnh đề `F.when().otherwise()` để map `age_at_death` thành các mức chuỗi biểu thị khoảng tuổi (`0-10`, `11-20`, `21-30`, `31-40`, `41-50`, `51-60`, `61-70`, `71-80`, `80+` và trạng thái `Unknown`).
   * **Aggregation (Nhóm lại):** Sử dụng `groupBy("Age_Range", "gender")` rồi áp dụng hàm `.agg(F.count("patient_id").alias("mortality_count"))`, sắp xếp lại theo tuổi và giới tính để trực quan khi truy vấn.
   * **Ghi Iceberg Table:** Sử dụng Method `writeTo(MORTALITY_TABLE).createOrReplace()` để xuất Dữ liệu Iceberg chuẩn lưu vào HDFS/S3, cho phép thay thế (ghi đè) dễ dàng nếu chạy lại job.

3. **Xác minh (Verification Phase):**
   * Trong mã đã thêm các step log `print` chi tiết cùng với bước hiển thị Metadata bảng sau khi ghi (`spark.catalog.tableExists`) tương tự như module phần 1, giúp tự động báo lỗi nếu quá trình ghi bị ngắt quãng.

Mã nguồn đã sẵn sàng để được chạy thông qua hệ thống Spark/Docker của dự án.
