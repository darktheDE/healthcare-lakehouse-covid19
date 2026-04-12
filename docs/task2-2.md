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
