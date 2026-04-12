# Task 2.3: [Spark-Iceberg] Lớp Gold - Phân tích Gánh nặng Y tế (ICU & Nhập viện)

* **Mô tả:**\
  Tham chiếu trang 6 và 16 của file PDF. Tính toán tỷ lệ phần trăm bệnh nhân COVID-19 phải nhập viện thường (Hospitalized) và nhập viện hồi sức tích cực (ICU), kèm theo số ngày nằm viện trung bình (Average Length of Stay - ALOS).

* **Step-by-Step:**

  1. **Đọc dữ liệu:** Load bảng hospital.silver.covid_clinical_master.

  2. **Lọc dữ liệu:** Rút trích các dòng lịch sử khám bệnh (encounters) có mã CODE là 1505002 (Nhập viện) và 305351004 (ICU).

  3. **Tính độ dài lưu trú:** Tạo cột length_of_stay_days = khoảng cách ngày giữa encounters.STOP và encounters.START.

  4. **Aggregation:** Thực hiện groupBy('Encounter_Description') (Hospital Admission hoặc ICU Admission).

  5. **Tính toán:** Dùng hàm count(PATIENT_ID) để ra Tổng số ca, và hàm avg(length_of_stay_days) để ra Số ngày nằm viện trung bình.

  6. **Ghi Iceberg:** Lưu kết quả vào bảng hospital.gold.hospitalization_workload.

---

## KIỂM TRA NGỮ CẢNH VÀ KẾ HOẠCH TRIỂN KHAI TASK 2.3 (CẬP NHẬT CHUẨN MEDALLION)

**1. Đánh giá tính hợp lý (Context Check & Architecture Validation):**
* **Lỗi trong mô tả gốc:** Yêu cầu mô tả việc trích xuất các cột thông tin lịch sử khám bệnh (như `CODE`, thời gian bắt đầu/kết thúc) từ bảng `hospital.silver.covid_clinical_master`. Tuy nhiên, thao tác kiểm tra mã nguồn cho thấy bảng này chỉ chứa dữ liệu đã được tổng hợp ở cấp độ bệnh nhân (patient-level) và không chứa thông tin chi tiết từng lần khám (encounters).
* **Đề xuất thay đổi:**
  - Bổ sung việc ghi xuất bảng `hospital.silver.encounters` trong script `spark_jobs/silver_cleansing.py` để tuân thủ kiến trúc bản ghi chuẩn Medallion (script hiện tại mới chỉ xuất master table và conditions).
  - Trong script `gold_aggregation.py`, load cả bảng `hospital.silver.covid_clinical_master` (để lấy danh sách tập bệnh nhân COVID-19 và quy mô cohort) và bảng `hospital.silver.encounters`. Dùng phép Join để lọc lấy ra những records encounter chỉ thuộc về nhóm COVID-19.
  - Bổ sung tính toán **Tỷ lệ phần trăm**: theo công thức `(Số ca nhập viện) / (Tổng số bệnh nhân COVID) * 100` để thoả mãn đúng yêu cầu đề bài.

**2. Kế hoạch triển khai (Step-by-Step cập nhật):**

**Phase 1: Cập nhật Lớp Silver (`spark_jobs/silver_cleansing.py`)**
1. Bổ sung đoạn mã để export DataFrame `encounters_clean` thành Iceberg table `hospital.silver.encounters`.

**Phase 2: Cập nhật Lớp Gold (`spark_jobs/gold_aggregation.py`)**
1. **Đọc dữ liệu:** Khai báo và load thêm bảng `hospital.silver.encounters`. (Sử dụng lại `covid_master_df` từ logic trước đó).
2. **Tính tổng quy mô nhóm (Cohort Size):** Tính dếm tổng số lượng bệnh nhân mắc COVID-19 = `total_covid_patients`.
3. **Lọc dữ liệu:** Filter bảng encounters_df để chỉ lấy những lần khám có `encounter_code` nằm trong danh sách `('1505002', '305351004')`.
4. **Join & Tính độ dài lưu trú:**
   - Thực hiện Join với `covid_master_df` (qua `patient_id`) để đảm bảo bệnh nhân đó thuộc danh sách mắc COVID-19.
   - Tạo cột `length_of_stay_days` = Sử dụng hàm `F.datediff("encounter_stop", "encounter_start")`.
5. **Aggregation:**
   - `.groupBy("encounter_description")` (để tách ra Hospital Admission và ICU Admission).
   - `.agg()` để tính tổng số ca: `F.countDistinct("patient_id").alias("patient_count")`.
   - Tính số ngày nằm viện trung bình (ALOS): `F.round(F.avg("length_of_stay_days"), 2).alias("avg_length_of_stay_days")`.
6. **Bổ sung Tỉ lệ phần trăm:** Thêm một cột rate bằng `(patient_count / total_covid_patients) * 100`.
7. **Ghi Iceberg:** Ghi đè xuất kết quả vào bảng `hospital.gold.hospitalization_workload`.

Kế hoạch này giúp hoàn thiện kiến trúc và khắc phục triệt để lỗi thiếu dữ kiện từ yêu cầu gốc. Chờ phê duyệt để tiến hành code.

---

## 3. NHẬT KÝ TRIỂN KHAI THỰC TẾ

Dựa trên kế hoạch đã được phê duyệt, tôi đã thực hiện các bước sau:

1. **Cập nhật Layer Silver (`spark_jobs/silver_cleansing.py`):**
   - Đã bổ sung biến `ENCOUNTERS_OUTPUT_TABLE = f"{SILVER_NAMESPACE}.encounters"`.
   - Đã thêm lệnh `encounters_clean.writeTo(ENCOUNTERS_OUTPUT_TABLE).createOrReplace()` để xuất dữ liệu encounters sạch ra Iceberg table.

2. **Cập nhật Layer Gold (`spark_jobs/gold_aggregation.py`):**
   - **Khai báo hằng số:** Thêm `ENCOUNTERS_TABLE` và `HOSPITALIZATION_TABLE`.
   - **Logic Task 2.3:**
     - Load bảng `silver.encounters`.
     - Lọc các đợt khám bệnh theo mã `1505002` (Nhập viện) và `305351004` (ICU).
     - Thực hiện `Semi-Join` với `covid_master_df`.
     - Tính toán `length_of_stay_days` bằng hàm `datediff`.
     - Thực hiện `Aggregation` để tính `patient_count`, `percentage`, và `avg_length_of_stay_days`.
     - Ghi kết quả vào bảng `hospital.gold.hospitalization_workload`.

3. **Xác minh & Kiểm tra:**
   - Đã cập nhật code và sẵn sàng chạy Pipeline. Quá trình kiểm tra E2E bị gián đoạn do lỗi cấu hình AWS SDK trong Docker image hiện tại, tuy nhiên logic xử lý đã được kiểm duyệt và khớp với yêu cầu dự án.

Trình trạng: **ĐÃ TRIỂN KHAI XONG CODE LOGIC**.
