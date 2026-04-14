# Task 3.3 Guide: Xây dựng BI Dashboard trên Apache Superset

Tài liệu này mô tả đúng cấu hình dashboard đã triển khai thành công trên Superset, bao gồm:

- Cài đặt và khởi động Apache Superset bằng Docker Compose.
- Kết nối Superset với Trino.
- Thiết kế 3 biểu đồ thực tế trong dashboard.
- Cách tạo Virtual Dataset cho biểu đồ `COVID Pathway Distribution`.
- Vì sao không thể dùng trực tiếp bảng Gold gốc để vẽ pie chart theo cách mong muốn.

## 1. Mục tiêu Task 3.3

- Dùng dữ liệu Gold trong catalog `iceberg` để trực quan hóa.
- Tạo dashboard phục vụ phân tích lâm sàng COVID.
- Đảm bảo biểu đồ phản ánh đúng cấu trúc dữ liệu hiện có trong repo.

## 2. Kết quả dashboard đã triển khai

Dashboard hiện tại gồm 3 biểu đồ chính:

1. `Cohort by Age Group` (Bar chart).
2. `Mortality by Age and Gender` (Heatmap).
3. `COVID Pathway Distribution` (Pie chart từ Virtual Dataset).

Nguồn dữ liệu sử dụng:

- `iceberg.gold.mortality_by_demographics`
- `iceberg.gold.covid_pathway_summary`
- Virtual Dataset được tạo từ `iceberg.gold.covid_pathway_summary`

## 3. Cài đặt và khởi động Apache Superset

### 3.1. Điều kiện trước khi cài

- Máy đã cài Docker và Docker Compose.
- Repo đã có file `.env` hợp lệ.
- File `deploy/docker-compose.yml` đã có service `superset`.

### 3.2. Build image Superset

Chạy từ thư mục gốc repo:

```bash
docker compose --env-file .env -f deploy/docker-compose.yml build superset --no-cache
```

Mục đích:

- Cài driver `sqlalchemy-trino` đúng vào image Superset.
- Tránh cache image cũ làm thiếu driver khi test connection.

### 3.3. Khởi động service Superset

```bash
docker compose --env-file .env -f deploy/docker-compose.yml up -d superset
```

Nếu cần chạy cùng Trino:

```bash
docker compose --env-file .env -f deploy/docker-compose.yml up -d trino superset
```

### 3.4. Truy cập Superset UI

- URL: `http://localhost:8088`
- Username mặc định: `admin`
- Password mặc định: `admin`

## 4. Kết nối Superset với Trino

### 4.1. SQLAlchemy URI

```text
trino://trino@trino:8080/iceberg
```

### 4.2. Lưu ý quan trọng

- Cần có username trong URI (`trino@...`), nếu không sẽ gặp lỗi 401.
- Superset và Trino chạy cùng Docker network nên host là `trino`, port nội bộ là `8080`.
- Catalog đang dùng là `iceberg`.

## 5. Hướng dẫn step-by-step tạo dashboard

### Bước 1: Tạo dataset gốc

Vào `Data -> Datasets -> + Dataset`, tạo 2 dataset:

1. `gold.mortality_by_demographics`
2. `gold.covid_pathway_summary`

### Bước 2: Tạo chart `Cohort by Age Group`

- Dataset: `gold.mortality_by_demographics`
- Chart type: `Bar Chart` (thanh ngang hoặc dọc đều được)
- Dimension: `age_group`
- Metric: `SUM(total_covid_patients)`
- Tên chart: `Cohort by Age Group`

Khuyến nghị:

- Bật hiển thị nhãn số trên cột để dễ đọc.
- Sắp xếp theo thứ tự nhóm tuổi nghiệp vụ (`0-18`, `19-34`, `35-49`, `50-64`, `65+`) nếu cần.

Chart này trả lời câu hỏi gì?

- Nhóm tuổi nào có quy mô cohort lớn nhất?
- Chênh lệch quy mô giữa các nhóm tuổi là bao nhiêu?
- Nhóm tuổi nào cần ưu tiên nguồn lực theo số lượng bệnh nhân?

### Bước 3: Tạo chart `Mortality by Age and Gender`

- Dataset: `gold.mortality_by_demographics`
- Chart type: `Heatmap`
- Trục X: `age_group`
- Trục Y: `gender`
- Metric: `AVG(mortality_rate_percent)` hoặc dùng trực tiếp `mortality_rate_percent` tùy cấu hình chart
- Tên chart: `Mortality by Age and Gender`

Khuyến nghị:

- Bật nhãn trong ô heatmap để hiển thị giá trị phần trăm.
- Dùng color scale liên tục để thấy rõ vùng nguy cơ cao.

Chart này trả lời câu hỏi gì?

- Nhóm tuổi nào có tỷ lệ tử vong cao nhất?
- Trong cùng nhóm tuổi, khác biệt giữa nam và nữ ra sao?

### Bước 4: Tạo Virtual Dataset cho `COVID Pathway Distribution`

Vào `SQL -> SQL Lab`, chọn database Trino, chạy truy vấn sau:

```sql
SELECT 'recovered' AS pathway, recovered_cases AS value
FROM iceberg.gold.covid_pathway_summary
UNION ALL
SELECT 'home_isolation' AS pathway, home_isolation_cases AS value
FROM iceberg.gold.covid_pathway_summary
UNION ALL
SELECT 'hospitalized' AS pathway, hospitalized_cases AS value
FROM iceberg.gold.covid_pathway_summary
UNION ALL
SELECT 'ventilation' AS pathway, ventilation_cases AS value
FROM iceberg.gold.covid_pathway_summary
UNION ALL
SELECT 'death' AS pathway, death_cases AS value
FROM iceberg.gold.covid_pathway_summary
UNION ALL
SELECT 'icu' AS pathway, icu_cases AS value
FROM iceberg.gold.covid_pathway_summary;
```

Sau khi query trả kết quả:

1. Chọn `Save -> Save dataset`.
2. Đặt tên dataset, ví dụ: `gold.covid_pathway_distribution_vds`.

### Bước 5: Tạo chart `COVID Pathway Distribution`

- Dataset: `gold.covid_pathway_distribution_vds` (Virtual Dataset vừa tạo)
- Chart type: `Pie Chart` hoặc `Donut Chart`
- Dimension: `pathway`
- Metric: `SUM(value)`
- Tên chart: `COVID Pathway Distribution`

Chart này trả lời câu hỏi gì?

- Tỷ trọng các trạng thái điều trị/kết cục hiện tại là như thế nào?
- Home isolation hay hospitalized đang chiếm phần lớn?
- Tỷ trọng các trạng thái nặng (`icu`, `ventilation`, `death`) đang ở mức nào?

### Bước 6: Tạo dashboard và ghép chart

Vào `Dashboards -> + Dashboard`:

- Tên gợi ý: `COVID Clinical Overview`
- Thêm 3 chart đã tạo:
  1. `Cohort by Age Group`
  2. `Mortality by Age and Gender`
  3. `COVID Pathway Distribution`

Sắp xếp layout:

- Hàng trên: `Cohort by Age Group`
- Hàng dưới trái: `Mortality by Age and Gender`
- Hàng dưới phải: `COVID Pathway Distribution`

## 6. Vì sao phải dùng Virtual Dataset cho COVID Pathway Distribution

Không nên vẽ pie chart trực tiếp từ bảng `iceberg.gold.covid_pathway_summary` theo dạng gốc vì:

1. Bảng gốc là dạng wide, chỉ 1 dòng và nhiều cột số đo (`home_isolation_cases`, `hospitalized_cases`, `recovered_cases`, ...).
2. Pie chart trong Superset cần dữ liệu dạng long: mỗi dòng là một nhóm (`pathway`) và một giá trị (`value`).
3. Virtual Dataset (unpivot bằng `UNION ALL`) giúp chuyển wide -> long để Superset hiểu đúng cấu trúc category/value.
4. Cách này giúp đặt lại nhãn nhóm rõ ràng và tái sử dụng cho dashboard.

Lưu ý nghiệp vụ:

- Tổng trên pie chart có thể lớn hơn `total_covid_cases` nếu bạn gộp các chỉ số không loại trừ nhau tuyệt đối (ví dụ `ventilation` là tập con của `hospitalized`).
- Vì vậy chart này nên được hiểu là phân bố các tín hiệu pathway/outcome đã chọn, không phải luôn là phân hoạch rời nhau 100%.

## 7. Truy vấn kiểm tra nhanh dữ liệu trước khi vẽ

### 7.1. Kiểm tra bảng mortality

```sql
SELECT *
FROM iceberg.gold.mortality_by_demographics
ORDER BY age_group, gender;
```

### 7.2. Kiểm tra bảng pathway summary

```sql
SELECT *
FROM iceberg.gold.covid_pathway_summary;
```

### 7.3. Kiểm tra virtual dataset (preview)

```sql
SELECT *
FROM (
  SELECT 'recovered' AS pathway, recovered_cases AS value
  FROM iceberg.gold.covid_pathway_summary
  UNION ALL
  SELECT 'home_isolation' AS pathway, home_isolation_cases AS value
  FROM iceberg.gold.covid_pathway_summary
  UNION ALL
  SELECT 'hospitalized' AS pathway, hospitalized_cases AS value
  FROM iceberg.gold.covid_pathway_summary
  UNION ALL
  SELECT 'ventilation' AS pathway, ventilation_cases AS value
  FROM iceberg.gold.covid_pathway_summary
  UNION ALL
  SELECT 'death' AS pathway, death_cases AS value
  FROM iceberg.gold.covid_pathway_summary
  UNION ALL
  SELECT 'icu' AS pathway, icu_cases AS value
  FROM iceberg.gold.covid_pathway_summary
) t
ORDER BY pathway;
```

## 8. Sự cố thường gặp và cách xử lý nhanh

### 8.1. Lỗi driver Trino

- Dấu hiệu: `Could not load database driver: TrinoEngineSpec`
- Cách xử lý: build lại image Superset có `sqlalchemy-trino` theo Dockerfile custom.

### 8.2. Lỗi 401 khi test connection

- Dấu hiệu: `Basic authentication or X-Trino-User must be sent`
- Cách xử lý: dùng URI có user, ví dụ `trino://trino@trino:8080/iceberg`.

### 8.3. Pie chart không hiển thị đúng

- Nguyên nhân: dùng trực tiếp bảng wide 1 dòng.
- Cách xử lý: tạo Virtual Dataset dạng long như mục 5.

## 9. Kết luận

Dashboard hiện tại đã triển khai đúng theo dữ liệu Gold thực tế của repo và có thể dùng cho báo cáo phân tích lâm sàng COVID. Điểm quan trọng nhất là biểu đồ `COVID Pathway Distribution` cần Virtual Dataset để mô hình dữ liệu phù hợp với cách Superset dựng pie chart.
