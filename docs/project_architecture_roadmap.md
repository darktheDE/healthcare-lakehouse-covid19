# 🏛 TỔNG QUAN KIẾN TRÚC & TIẾN ĐỘ DỰ ÁN HEALTHCARE LAKEHOUSE

Tài liệu này tóm tắt bức tranh toàn cảnh về kiến trúc Data Lakehouse bạn đang xây dựng, các thành thạo kỹ thuật/vấn đề khó chúng ta đã giải quyết cùng nhau tính đến hiện tại, và lộ trình các bước tiếp theo để đưa kiến trúc này lên mức Hoàn Hảo (High Distinction) cho đồ án.

## 1. Kiến Trúc Hệ Thống (Lakehouse Architecture)
Hệ thống được thiết kế chặt chẽ theo tiêu chuẩn công nghiệp **Medallion Architecture**, sử dụng tinh hoa của hệ sinh thái mã nguồn mở thế đại mới:

*   **Data Source (OLTP):** Dùng `PostgreSQL`. Đóng vai trò là phần mềm mô phỏng hệ thống bệnh viện nội bộ, nơi sản sinh ra hàng triệu dòng dữ liệu khám chữa bệnh thô tĩnh (Patients, Encounters, Conditions).
*   **Object Storage (Datalake):** Dùng `MinIO`. Hoạt động tương đương AWS S3 bản On-Premise. Là trái tim lưu trữ vật lý chi phí thấp khổng lồ, chia thành 4 vùng chứa: `raw/`, `bronze/`, `silver/`, `gold/`.
*   **Processing Engine & Table Format:** Dùng `Apache Spark` thao tác cực mạnh trên định dạng `Apache Iceberg`. Việc dùng Iceberg giúp Đầm lầy dữ liệu (Datalake) rời rạc kia có được sức mạnh ACID của SQL truyền thống (Cập nhật, Xóa, Ghi ngược thời gian/Time-travel) mà không sợ hỏng tệp.
*   **Data Catalog:** Nhờ `Hive Metastore` trỏ vào lưu siêu dữ liệu (Metadata) dưới con `metastore-db`. Đây là cuốn bách khoa toàn thư. Khi Spark muốn truy cập Iceberg, thay vì phải quét hàng trăm GB nằm lung tung trên MinIO, nó chỉ cần hỏi Metastore là biết rành rọt thông tin vị trí các cột, kích thước tệp và đường dẫn s3!
*   **Orchestration (Tương lai):** Sử dụng `Apache Airflow` làm quản gia lập lịch.
*   **Query Engine (Tương lai):** Dùng `Trino` đứng rào chắn phía trước. Các người dùng cuối (Business Analyst/BI) sẽ gõ SQL ở đây để phân tích bảng Iceberg tốc độ cao.

---

## 2. Những Gì Chúng Ta Đã Nâng Cấp & Gỡ Lỗi Thành Công
Chúng ta không chỉ làm một đồ án bình thường, mà đang ứng dụng kỹ năng của Kỹ sư DevOps & Data để tối ưu quy trình từ Manual rườm rà thành Fully Automated (Tự động hóa hoàn toàn 100%).

1.  **Chống lộ lọt thông tin & Tối ưu Tài Nguyên:**
    *   Toàn bộ cấu hình nhạy cảm (Tên, User, Password, Mật mã S3) đã được trừu tượng hóa và tống xuất vào một pháo đài `.env` riêng biệt, không ai đọc được trong mã nguồn chính.
    *   Điều hướng RAM hợp lý, gộp tải giúp máy tính cá nhân không bị quá sức (Out of memory) khi kéo cả 10 công nghệ cùng lúc.
2.  **Tự Động Hóa Data Source & MinIO (Task 1.2 & 1.3):**
    *   Viết logic bắt `postgres-source` tự đổ bộ hàng loạt Schema hệ thống bệnh viện và nạp `COPY` hàng GB Data ngay khi khởi động nhờ script gắp vào `/docker-entrypoint-initdb.d/`.
    *   Tạo container dùng một lần (`mc`). Nó bí mật đăng nhập Admin MinIO, tạo cấu trúc thư mục Datalake rành mạch, bảo mật và cấp quyền truy cập Lakehouse Admin cực kỳ ngầu mà con người không một thao tác chuột.
3.  **Điều Tra & Sửa Lỗi Ngầm Của Tabulario (Fix Hive-Metastore):**
    *   Hệ thống liên tục văng lỗi `Connection refused` hoặc `UnknownHost`. Sau khi lần mò đọc logs sâu thẳm, chúng ta phát hiện nguyên căn là Image Hive-Metastore của Tabulario đang bật sai dịch vụ (Nó bật HiveServer2 thay vì Hive Metastore chuyên dụng để tiếp dân hệ Iceberg).
    *   Gỡ lỗi bằng một lệnh nhúng `sed` can thiệp thẳng vào ruột Container lúc nó đang load, ép nó phải phục vụ ở chuẩn port `9083`.
4.  **Khai Thác Thô Hệ Sinh Thái (Ingestion Bằng Spark - Bronze Task 1.5):**
    *   Viết quy trình `bronze_ingestion.py` kéo đủ Dependency thiếu (Gói `hadoop-aws` từ kho Maven), xử lý file béo phì (Encounters 1GB) xuống thẳng phân vùng Iceberg.
    *   Lập trình ra một Docker container tên `spark-bronze-job`. Nó như lính đánh thuê đâm vào hệ thống, chực chờ Hive khởi động, cắn dữ liệu nhai thành Bronze, sau đó tự biến mất để hoàn trả RAM. 

---

## 3. Các Bước Đi Tiếp Theo (Future Work Roadmap)
Gốc rễ phần Mạ vàng Hạ Tầng đã quá vững và hoàn hảo. Các giai đoạn tiếp theo thuần túy là Scale Logic xử lý trong file Python:

### Chặng A: Silver Layer - Dọn Bể Rác Data
Từ đống lộn xộn ở lớp Bronze chúng ta cần:
*   **Làm Sạch (Data Cleansing):** Filter ném đi các bệnh nhân (Patients) rỗng ID, loại bỏ các tình trạng sức khỏe (Conditions) không hợp lệ, xử lý lại chuỗi (Upper/Lower case).
*   **Casting Kiểu Dữ Liệu:** Đưa các chữ số tiền viện phí từ String sang Decimal; chuyển chuỗi ngày tháng dài ngằng sang Timestamp.
*   **Nâng Cấp:** Dùng kỹ thuật Partitioning của Iceberg (VD: Xẻ nhỏ file theo năm hoặc EncounterClass). Cứ Code ở Pyspark `writeTo().partitionedBy(...)` thay vì cứ xả như đống bùn.

### Chặng B: Gold Layer - Khai Phá Kim Cương (Mô hình hóa / BI)
*   **Dimensional Modeling:** Áp dụng mô hình thiết kế Star Schema (Fact/Dimension table). Ở đoạn này, bạn sẽ dùng Python cho hợp nhất (JOIN) thông tin giữa Encounters và Patients. 
*   Mục đich cuối là đẻ ra những cụm bảng Thống kê "Tỷ lệ tử vong tại khoa cấp cứu" hoặc "Lợi nhuận viện phí theo Khu vực" làm nguyên liệu cho mảng Báo Cáo.

### Chặng C: Airflow Pipeline (Linh Hồn Thực Sự Của Tự Động Hóa)
*   Hiện tại Container `spark-bronze-job` chỉ giải quyết lệnh chạy một lần (Batch thô).
*   Tại doanh nghiệp, dữ liệu sẽ tuôn chảy theo ngày. Chúng ta sẽ mở Dashboard Apache Airflow (Cổng 8082), viết code DAG `PythonOperator` kết hợp `SparkSubmitOperator` với dòng lệnh dạng: *Cứ lặp lại đúng 3h sáng, Chạy Bronze Python, làm xong báo cáo xanh lá... Kích hoạt tiếp quy trình Silver và nhai Gold.*

### Chặng D: BI/Analytics Layer
*   Triệu hồi Trino bằng Dbeaver hoặc cắm Metabase/Power BI. Bán số liệu ra thành Graph hình ảnh (Dùng đúng cấu trúc SQL bình thường từ catalog `hospital.gold.xxx`).



1. Kiến Trúc "Medallion Lakehouse" Của Chúng Ta Hoạt Động Ra Sao?
Thay vì lưu dữ liệu vào Data Warehouse truyền thống đắt đỏ, chúng ta đang dựng một hệ thống Big Data linh hoạt:

Tầng Rễ (Postgres-Source): Cứ coi nó là Database trung tâm của bệnh viện, nơi bác sĩ trực tiếp nhập dữ liệu bệnh nhân.
Biển Hồ (MinIO): Là một phân vùng cất trữ File (SaaS) mô phỏng AWS S3. Nó rẻ, không giới hạn.
Trái Tim Xử Lý (Spark) & Cấu Trúc Bảng (Iceberg): MinIO chỉ biết lưu File. Để MinIO có tính năng của một Database xịn (Có thể SELECT, INSERT, UPDATE, TIME-TRAVEL), ta nhúng format Iceberg vào. Spark đóng vai trò là Lò mổ, nhận dữ liệu chép vào chuẩn Iceberg tốc độ cao.
Cuốn Danh Bạ (Hive Metastore): Lưu "Bản Đồ". Khi có query muốn đọc dữ liệu patients, Spark sẽ không đi tìm mù quáng trên S3, nó qua hỏi Metastore: "Data của bệnh nhân nằm ở đường dẫn S3 nào?", Metastore nhả vị trí ra, Spark cứ thế chạy thẳng tới nhặt file đem đi tính toán.
Đầu Não Giao Diện (Trino & Superset): Là "Mặt tiền" của hệ thống, giúp Data Analyst gõ code SQL thay vì code Python phức tạp, hoặc bấm kéo thả làm báo cáo BI.
2. Các Mấu Chốt Quan Trọng Bạn Đã Gỡ Lỗi Và Làm Chủ
Bảo Mật Bằng Cách Giấu Settings: Cách ly hoàn toàn mật khẩu nhạy cảm (.env), không quăng tài khoản vào thẳng luồng code chung nữa.
Khởi Tạo Tự Động Đầu Nguồn: Code postgres_init.sql và Container siêu nhỏ ngầm mc giúp tự động kéo Schema Bệnh Viện và phân quyền Datalake Bucket mà khỏi tốn công người lập trình.
Cứu Chữa Hỏng Hóc Hive-Metastore của Tabulario: Image đó bật sai dịch vụ và giấu kín Cổng kết nối port 9083. Chúng ta dùng kĩ nghệ sửa file ngay khi đang boot (sed) để ép Hive phải chịu mở Metastore Server. Quá vi diệu!
Viết Vòng Lặp Ingestion Spark Chạy Nền: Container spark-bronze-job giống như người thay ca đêm, giúp project của bạn từ nay auto 100% khi khởi chạy. Vứt File 1GB qua cho nó cũng nhai cực mượt!
3. Con Số Và Tương Lai Bước Tiếp Thế Nào?
Bước Silver: Hiện tại Data ở mức Bronze là toàn rác và lỗi. Bước sau phải lập trình Spark dọn sạch cột Date bị Null, ép kiểu tiền viện phí về dạng Decimal, rẽ nhánh dữ liệu phân mảnh (Partitioning) file.
Bước Gold: Code join dữ liệu từ Encounters sang Patients để lập các bảng Aggregation Báo cáo doanh thu và Phân tích tệp bệnh án chuyên sâu. (Star schema Data Modeling).
Trùm Cuối Airflow: Gỡ bỏ những thao tác gọi thủ công. Khiển Apache Airflow quy định "12 giờ đêm nay thì mài Bronze, 1 giờ sáng qua Silver. Lỗi ở đâu báo Slack về ở đó".