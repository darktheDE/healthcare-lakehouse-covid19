# Task 3.1 Guide: Trino SQL Serving (Iceberg + Hive Metastore + MinIO)

Tài liệu này tổng hợp những gì đã làm để đưa Trino vào lớp SQL Serving, các khó khăn gặp phải, cách xử lý, và hướng dẫn cấu hình + truy vấn Iceberg trên MinIO.

---

## 1. Muc tieu

- Ket noi Trino toi Hive Metastore de doc metadata Iceberg.
- Truy van du lieu Bronze, Silver, Gold bang SQL (CLI/DBeaver).
- Dam bao pipeline chay xong la Trino thay duoc schema/table.

---

## 2. Nhung viec da lam

### 2.1. Them Trino vao docker-compose

- Them service `trino` (image `trinodb/trino:480`).
- Map port `8081:8080` de truy cap tu host.
- Mount catalog: `./trino/catalog:/etc/trino/catalog`.

### 2.2. Patch Hive Metastore de chay dung dich vu

- Image `tabulario/hive-metastore` mac dinh chay `hiveserver2` thay vi `metastore`.
- Patch entrypoint theo cach an toan: sua `run.sh` de van giu buoc init schema, sau do chay metastore.

Trong `docker-compose.yml`:
```
entrypoint: ["/bin/sh", "-c", "sed -i 's#/opt/hive/bin/hiveserver2#/opt/hive/bin/hive --service metastore#g' /app/run.sh; /app/run.sh"]
```

### 2.3. Cap nhat Spark job sang Hive catalog

- Tranh tinh trang Trino khong thay bang do metadata chi nam tren S3.
- Chuyen `spark.sql.catalog.hospital.type` tu `hadoop` sang `hive`.
- Bo sung `spark.sql.catalog.hospital.uri=thrift://hive-metastore:9083`(áp dụng với tất cả các layer trong scripts/)

Da cap nhat:
- `deploy/scripts/bronze_ingestion.py`
- `deploy/scripts/silver_cleansing.py`
- `deploy/scripts/gold_clinical_analysis.py`

---

## 3. Kho khan gap phai va cach xu ly

### Van de 1: Trino khong ket noi duoc Hive Metastore

**Trieu chung:**
- `Failed connecting to Hive metastore: [hive-metastore:9083]`

**Nguyen nhan:**
- Container Hive Metastore dang chay HiveServer2 (sai dich vu).

**Cach khac phuc:**
- Patch entrypoint nhu muc 2.2 de khoi dong dung metastore.

---

### Van de 2: Khong co schema bronze/silver/gold trong Trino

**Trieu chung:**
- `Schema 'bronze' does not exist`

**Nguyen nhan:**
- Spark job dung Hadoop catalog, metadata chi nam tren MinIO (S3) ma khong dang ky vao Hive Metastore.

**Cach khac phuc:**
- Chuyen Spark sang Hive catalog (muc 2.3).
- Neu da co data truoc do, co the dang ky bang tay bang `register_table` (xem muc 6.2).

---

### Van de 3: `register_table` bi disable

**Trieu chung:**
- `register_table procedure is disabled`

**Cach khac phuc (tuy chon):**
- Them dong sau vao `deploy/trino/catalog/iceberg.properties`, sau do restart Trino:
```
iceberg.register-table-procedure.enabled=true
```

---

## 4. Cau hinh Trino Iceberg (bat buoc)

File: `deploy/trino/catalog/iceberg.properties`
```
connector.name=iceberg
iceberg.catalog.type=HIVE_METASTORE
hive.metastore.uri=thrift://hive-metastore:9083
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.aws-access-key=lakehouse_admin
s3.aws-secret-key=Lakehouse123!
s3.path-style-access=true
s3.region=us-east-1
```

---

## 5. Su dung Trino (CLI va DBeaver)

### 5.1. Trino CLI

```bash
docker exec -it trino trino
```

### 5.2. Truy van kiem tra

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES IN iceberg.bronze;
SHOW TABLES IN iceberg.silver;
SHOW TABLES IN iceberg.gold;
```

### 5.3. DBeaver

- Host: `localhost`
- Port: `8081`
- User: bat ky (vd: `trino`)
- Catalog: `iceberg`

---

## 6. Dang ky bang Iceberg khi da co data cu
(phần này không cần quan tâm lắm :v)

Neu data da co tren MinIO nhung HMS chua co metadata, co the dang ky bang tay:

### 6.1. Tao schema voi location dung

```sql
CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location='s3a://hospital-lakehouse/warehouse/bronze');
CREATE SCHEMA IF NOT EXISTS iceberg.silver WITH (location='s3a://hospital-lakehouse/warehouse/silver');
CREATE SCHEMA IF NOT EXISTS iceberg.gold WITH (location='s3a://hospital-lakehouse/warehouse/gold');
```

### 6.2. Register table

```sql
CALL iceberg.system.register_table('bronze','patients','s3a://hospital-lakehouse/warehouse/bronze/patients/metadata/v1.metadata.json');
CALL iceberg.system.register_table('bronze','encounters','s3a://hospital-lakehouse/warehouse/bronze/encounters/metadata/v1.metadata.json');
CALL iceberg.system.register_table('bronze','conditions','s3a://hospital-lakehouse/warehouse/bronze/conditions/metadata/v1.metadata.json');

CALL iceberg.system.register_table('silver','covid_clinical_master','s3a://hospital-lakehouse/warehouse/silver/covid_clinical_master/metadata/v1.metadata.json');

CALL iceberg.system.register_table('gold','mortality_by_demographics','s3a://hospital-lakehouse/warehouse/gold/mortality_by_demographics/metadata/v1.metadata.json');
CALL iceberg.system.register_table('gold','covid_pathway_summary','s3a://hospital-lakehouse/warehouse/gold/covid_pathway_summary/metadata/v1.metadata.json');
```

---

## 7. Checklist sau khi chay pipeline

1. `SHOW SCHEMAS FROM iceberg` thay `bronze/silver/gold`.
2. `SHOW TABLES IN iceberg.gold` thay it nhat 2 bang Gold.
3. Co the query du lieu mau:

```sql
SELECT count(*) FROM iceberg.bronze.patients;
SELECT count(*) FROM iceberg.silver.covid_clinical_master;
SELECT * FROM iceberg.gold.mortality_by_demographics LIMIT 5;
```

---

## 8. Ghi nho nhanh

- Muon Trino thay bang sau moi pipeline: Spark phai dung **Hive catalog**.
- Hive Metastore phai chay dung dich vu (metastore) va phai on dinh.
- Khi gap loi schema khong ton tai: kiem tra metadata trong HMS hoac dang ky lai bang.
