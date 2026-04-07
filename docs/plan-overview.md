# TÀI LIỆU KẾ HOẠCH TỔNG QUAN

## 1. PHÂN CÔNG VAI TRÒ VÀ NHIỆM VỤ (ROLES & RESPONSIBILITIES)

_Nguyên tắc phân công:_ Để đáp ứng yêu cầu môn học, toàn bộ 4 thành viên đều tham gia viết code Apache Spark và tương tác với Hive Metastore/Iceberg trong lõi Medallion. Các module hạ tầng và ứng dụng sẽ được chia theo thế mạnh chuyên môn (DE & BE).

**1. Đỗ Kiến Hưng (Data Engineer)**

* _Nhiệm vụ lõi (Spark/Medallion):_ Cấu hình Spark Session kết nối với Iceberg/Hive. Viết code Spark xử lý lớp **Bronze** (Đọc từ Postgres/MinIO và ghi format Iceberg).

* _Nhiệm vụ chuyên môn (DE):_ Thiết kế Data Model (Schema), viết Airflow DAGs để điều phối toàn bộ pipeline, cấu hình kết nối Trino với Hive Metastore.

**2. Nguyễn Văn Quang Duy (Data Engineer)**

* _Nhiệm vụ lõi (Spark/Medallion):_ Viết code Spark xử lý lớp **Silver** (Data Cleansing: Xử lý null, cast data type, Join các bảng `patients`, `encounters`, `conditions`).

* _Nhiệm vụ chuyên môn (DE):_ Thiết lập cấu hình Hive Metastore Server. Thiết kế và tạo các biểu đồ (Dashboards) trên Apache Superset phục vụ báo cáo.

**3. Phan Trọng Quí (Backend Engineer)**

* _Nhiệm vụ lõi (Spark/Medallion):_ Viết code Spark xử lý lớp **Gold** - Bảng 1 (Aggregations: Viết logic tính toán tổng hợp về _Doanh thu chi phí khám chữa bệnh_ - Revenue Analysis).

* _Nhiệm vụ chuyên môn (BE):_ Thiết lập hạ tầng Docker Compose tổng thể (Networks, Volumes). Setup hệ thống PostgreSQL và nạp dữ liệu Synthea CSV vào DB.

**4. Phan Trọng Phú (Backend Engineer)**

* _Nhiệm vụ lõi (Spark/Medallion):_ Viết code Spark xử lý lớp **Gold** - Bảng 2 (Aggregations: Viết logic tính toán tổng hợp về _Xu hướng dịch bệnh COVID-19_ - Disease Trends).

* _Nhiệm vụ chuyên môn (BE):_ Thiết lập cấu hình Object Storage (MinIO). Phát triển hệ thống RESTful API bằng **FastAPI** kết nối với Trino để cung cấp dữ liệu cho Client.

## 2. DANH SÁCH CÁC MODULES (PROJECT MODULES)

_Trên Plane.so, đây sẽ là các _`Modules`_ để gom nhóm các Epic/Task liên quan._

* **Module 1: Infrastructure & Data Source (Hạ tầng & Dữ liệu nguồn)**

  * Tập trung vào việc dựng môi trường Docker, chuẩn bị MinIO, Hive Metastore và nạp dữ liệu Synthea vào PostgreSQL.

* **Module 2: Ingestion & Orchestration (Thu nhận & Điều phối)**

  * Xây dựng luồng Airflow DAG để trích xuất dữ liệu tự động.

* **Module 3: Medallion Lakehouse Processing (Lõi Xử lý dữ liệu)**

  * Phần quan trọng nhất: Viết các Job Apache Spark để chuyển hóa dữ liệu qua 3 lớp Bronze -> Silver -> Gold và lưu trữ bằng Apache Iceberg format.

* **Module 4: Data Serving & Consumption (Truy vấn & Cung cấp API)**

  * Cấu hình Trino Engine, vẽ Dashboard trên Superset và viết FastAPI Endpoint.

## 3. KẾ HOẠCH TRIỂN KHAI THEO CYCLES (SPRINTS)

_(Giả định mỗi Cycle tương đương 1 tuần làm việc, dù thực tế có thể co giãn. Plan này đảm bảo flow logic từ A-Z)._

### Cycle 1: Foundation & Ingestion (Xây nền móng và Đưa dữ liệu vào hồ)

_Mục tiêu:_ Dựng thành công hạ tầng Docker, nạp dữ liệu nguồn và đẩy được dữ liệu thô lên MinIO.

* **Task 1:** Viết file `docker-compose.yml` khởi tạo PostgreSQL, MinIO, Hive Metastore, Spark, Airflow (Người làm: Quí, Phú).

* **Task 2:** Khởi tạo Schema PostgreSQL và viết script nạp 100K record data Synthea CSV vào Database (Người làm: Quí).

* **Task 3:** Setup MinIO Buckets (tạo bucket `lakehouse`) và Access Keys (Người làm: Phú).

* **Task 4:** Viết Airflow DAG kết nối Postgres, extract dữ liệu lưu tạm xuống local worker hoặc đẩy trực tiếp lên MinIO (Người làm: Hưng).

* **Task 5 (Spark - Lớp Bronze):** Viết Spark Job đọc dữ liệu thô vừa extract, khai báo Catalog với Hive Metastore và ghi thành bảng Iceberg ở lớp Bronze (Người làm: Hưng).

### Cycle 2: The Medallion Architecture (Xử lý dữ liệu đa tầng)

_Mục tiêu:_ Hoàn thiện việc làm sạch và tổng hợp dữ liệu bằng Apache Spark. Mọi người đều phải tham gia review code Spark của nhau.

* **Task 1 (Spark - Lớp Silver):** Đọc data từ Bronze, thực hiện loại bỏ dữ liệu lỗi, chuyển đổi định dạng ngày tháng, thực hiện JOIN 3 bảng `patients`, `encounters`, `conditions`. Ghi đè lên MinIO dưới dạng bảng Iceberg lớp Silver (Người làm: Duy).

* **Task 2 (Spark - Lớp Gold 1):** Đọc data từ Silver, dùng Spark SQL hoặc DataFrame API để GROUP BY, tính tổng doanh thu/chi phí (`TOTAL_CLAIM_COST`) theo tháng/khu vực. Ghi thành bảng `gold_revenue` (Người làm: Quí).

* **Task 3 (Spark - Lớp Gold 2):** Đọc data từ Silver, đếm số ca nhiễm COVID-19 theo tuần/tháng/độ tuổi. Ghi thành bảng `gold_covid_trends` (Người làm: Phú).

* **Task 4:** Cập nhật Airflow DAG để móc nối các Job Spark này chạy tuần tự: `Ingest -> Bronze -> Silver -> Gold` (Người làm: Hưng).

### Cycle 3: Data Serving, API & Visualization (Truy xuất và Tiêu thụ)

_Mục tiêu:_ Cắm Engine truy vấn vào Data Lake và đưa dữ liệu đến tay người dùng cuối.

* **Task 1:** Dựng container Trino, cấu hình file `catalog/iceberg.properties` để Trino kết nối được với Hive Metastore và MinIO (Người làm: Hưng).

* **Task 2:** Test các câu lệnh SQL truy vấn trực tiếp bảng Gold trên giao diện CLI/DBeaver thông qua Trino (Người làm: Duy).

* **Task 3:** Dựng container Apache Superset, kết nối Database Connection tới Trino. Thiết kế 2 Dashboard trực quan từ 2 bảng Gold (Người làm: Duy).

* **Task 4:** Khởi tạo project FastAPI, dùng thư viện `trino-python-client` kết nối đến Trino. Viết 2 API GET endpoints trả về JSON data cho Client (Người làm: Phú, Quí).

* **Task 5:** Viết tài liệu README.md, dọn dẹp code, chuẩn bị slide và kịch bản Demo (Cả team).
