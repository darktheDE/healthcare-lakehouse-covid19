Task 1.2: Thiết lập Database nguồn (PostgreSQL) và Nạp dữ liệu Synthea
* **Description:** Chuẩn bị dữ liệu y tế thô trong môi trường quan hệ để giả lập hệ thống thật.

* **Steps by Step:**

  1. Truy cập vào container `postgres_source`.

  2. Tạo Database `hospital_db`.

  3. Viết script DDL (SQL) để tạo các bảng: `patients`, `encounters`, `conditions`.

  4. Sử dụng lệnh `COPY` trong SQL hoặc script Python để nạp dữ liệu từ các file CSV Synthea 100K (đã tải về) vào các bảng tương ứng.

  5. Thực hiện truy vấn kiểm tra số lượng record (Verify count) để đảm bảo đủ 100.000 bệnh nhân.
