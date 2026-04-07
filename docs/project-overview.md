# TÀI LIỆU TỔNG QUAN DỰ ÁN (PROJECT OVERVIEW)

## 1. THÔNG TIN CHUNG

* **Tên dự án:** Thiết kế và Triển khai Hệ thống Data Lakehouse phân tích dữ liệu y tế (COVID-19).

* **Môn học:** Phân tích dữ liệu lớn (BDAN333977).

* **Giảng viên hướng dẫn:** ThS. Lê Thị Minh Châu.

* **Nhóm thực hiện:** Nhóm 01 (Phan Trọng Phú, Phan Trọng Quí, Đỗ Kiến Hưng, Nguyễn Văn Quang Duy).

* **Mô hình phát triển:** Agile/Scrum.

* **Môi trường triển khai:** Docker (Containerization).

## 2. BỐI CẢNH VÀ MỤC TIÊU DỰ ÁN (BACKGROUND & OBJECTIVES)

**Bối cảnh:**\
Các hệ thống y tế hiện nay tạo ra một lượng dữ liệu khổng lồ (bệnh án, hóa đơn, thông tin chẩn đoán, v.v.). Tuy nhiên, dữ liệu thường bị phân mảnh trong các cơ sở dữ liệu quan hệ (OLTP) vốn chỉ tối ưu cho việc ghi chép (giao dịch) chứ không phù hợp để chạy các truy vấn phân tích (OLAP) phức tạp, dẫn đến tình trạng quá tải hệ thống và chậm trễ trong việc ra quyết định.

**Mục tiêu:**

* **Về mặt kỹ thuật:** Xây dựng một kiến trúc **Data Lakehouse** hoàn chỉnh, tách biệt hoàn toàn lớp lưu trữ (Storage) và lớp tính toán (Compute), sử dụng 100% công cụ mã nguồn mở. Hệ thống phải đảm bảo tính nhất quán của dữ liệu (ACID) và có khả năng mở rộng (Scalable) khi triển khai thực tế.

* **Về mặt nghiệp vụ:** Tự động hóa luồng trích xuất dữ liệu y tế, chuẩn hóa và tổng hợp dữ liệu để phục vụ hai mục đích chính: (1) Trực quan hóa xu hướng dịch bệnh và doanh thu qua Dashboard; (2) Cung cấp dữ liệu dưới dạng dịch vụ (Data-as-a-Service) thông qua API cho các ứng dụng nội bộ.

## 3. PHẠM VI DỰ ÁN (PROJECT SCOPE)

**Trong phạm vi (In-Scope):**

* Giả lập hệ thống nguồn (OLTP) chứa dữ liệu bệnh nhân và lịch sử khám chữa bệnh bằng PostgreSQL.

* Thiết lập Data Pipeline tự động trích xuất dữ liệu định kỳ (Batch Processing).

* Xây dựng kho lưu trữ Data Lakehouse áp dụng mô hình Medallion (Bronze - Silver - Gold).

* Triển khai công cụ truy vấn SQL phân tán tốc độ cao.

* Xây dựng BI Dashboard báo cáo (Doanh thu, Xu hướng bệnh) và hệ thống RESTful API.

**Ngoài phạm vi (Out-of-Scope):**

* Xử lý dữ liệu thời gian thực (Streaming Data / CDC). _(Có thể đưa vào hướng phát triển tương lai)_.

* Triển khai hệ thống phân quyền (IAM) bảo mật nâng cao và các giao thức HTTPS/SSL.

* Huấn luyện các mô hình Machine Learning/AI chuyên sâu trên tập dữ liệu.

## 4. KIẾN TRÚC VÀ CÔNG NGHỆ (ARCHITECTURE & TECH STACK)

Dự án tuân thủ kiến trúc 5 lớp của Data Lakehouse, được triển khai hoàn toàn trên **Docker Environment**:

1. **Data Source:** `PostgreSQL` (Giả lập hệ thống dữ liệu gốc của bệnh viện).

2. **Ingestion & Orchestration:** `Apache Airflow` (Điều phối quy trình ETL/ELT tự động).

3. **Storage & Table Format:**

   * Lưu trữ vật lý: `MinIO` (Object Storage tương thích AWS S3).

   * Định dạng bảng: `Apache Iceberg` (Cung cấp tính năng ACID, Time-travel).

   * Quản lý Metadata: `Hive Metastore` (Cầu nối giao tiếp giữa các engine).

4. **Data Processing:** `Apache Spark` (Engine phân tán dùng để làm sạch và tổng hợp dữ liệu qua 3 lớp Bronze -> Silver -> Gold).

5. **Serving & Consumption:**

   * Query Engine: `Trino` (Truy vấn SQL tốc độ cao trực tiếp trên MinIO).

   * BI & Visualization: `Apache Superset` (Dashboard quản trị).

   * Data API: `FastAPI` (Python RESTful API cung cấp endpoint cho client).

## 5. BỘ DỮ LIỆU (DATASET)

* **Nguồn cung cấp:** Synthea™ (Bộ dữ liệu bệnh nhân nhân tạo mã nguồn mở do MITRE phát triển, tuân thủ HIPAA).

* **Tập dữ liệu sử dụng:** COVID-19 100K (Mô phỏng dữ liệu lịch sử khám chữa bệnh của 100.000 bệnh nhân COVID-19).

* **Các bảng dữ liệu trọng tâm:**

  * `patients.csv` (Thông tin nhân khẩu học).

  * `encounters.csv` (Lịch sử đến khám và chi phí/doanh thu).

  * `conditions.csv` (Tình trạng bệnh/Mã bệnh chẩn đoán).

## 6. KẾT QUẢ KỲ VỌNG (EXPECTED DELIVERABLES)

1. **Source Code Repository:** Cấu trúc thư mục chuẩn bao gồm file `docker-compose.yml`, các scripts cấu hình, Airflow DAGs, Spark code (Python/Scala) và code FastAPI.

2. **Hệ thống chạy thực tế (Local/Cloud):** Các container chạy ổn định, có thể trình diễn luồng dữ liệu end-to-end từ PostgreSQL đến Dashboard.

3. **BI Dashboard:** Tối thiểu 2 báo cáo trực quan (Phân tích doanh thu và Xu hướng dịch bệnh COVID-19).

4. **Tài liệu báo cáo:** Báo cáo cuối kỳ (Word/PDF) và Slide thuyết trình (PowerPoint).
