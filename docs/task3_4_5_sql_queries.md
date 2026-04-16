# Các Truy Vấn SQL Phân Tích Dữ Liệu COVID-19 (Lớp Gold)

Dưới đây là các câu lệnh SQL để truy vấn trực tiếp từ các bảng Data Mart (Iceberg - lớp Gold) đã được hệ thống tổng hợp thông qua Spark Job `gold_aggregation.py`. Bạn có thể sử dụng các query này trong Superset, Trino hoặc Presto.

## Bài toán 3: Phân tích Triệu chứng và Biến chứng lâm sàng (Symptoms & Outcomes)

**Mục tiêu:** Tra cứu tỷ lệ các biến chứng nghiêm trọng giữa nhóm Sống sót và Tử vong từ bảng `symptoms_outcomes`.

```sql
SELECT 
    condition_category,
    is_survivor,
    patient_count,
    total_group_population,
    rate_percentage
FROM hospital.gold.symptoms_outcomes
ORDER BY condition_category, is_survivor;
```

---

## Bài toán 4: Phân tích Tỷ lệ tử vong chi tiết theo Thập kỷ (Mortality Demographics)

**Mục tiêu:** Tra cứu khối lượng bệnh nhân tử vong phân theo dải tuổi 10 năm và giới tính từ bảng `mortality_demographics`.

```sql
SELECT 
    Age_Range,
    gender,
    mortality_count
FROM hospital.gold.mortality_demographics
ORDER BY Age_Range, gender;
```

---

## Bài toán 5: Phân tích Gánh nặng Y tế và Thời gian lưu trú (ALOS)

**Mục tiêu:** Tra cứu tỷ lệ nhập viện thường/ICU và số ngày nằm viện trung bình của bệnh nhân COVID từ bảng `hospitalization_workload`.

```sql
SELECT 
    encounter_description,
    patient_count,
    avg_length_of_stay_days,
    percentage
FROM hospital.gold.hospitalization_workload
ORDER BY percentage DESC, encounter_description;
```
