# Task 3.2 Guide: Kiem dinh du lieu va Toi uu truy van tren Trino

Tai lieu nay ghi lai ket qua thuc thi Task 3.2 tren moi truong hien tai, kem SQL templates de ban giao cho team Dashboard/API.

## 1. Context sau khi doi chieu docs

- Theo docs moi nhat [Task 3.1], Trino dang doc catalog `iceberg` qua Hive Metastore.
- Trong Gold schema hien tai, Trino nhin thay 2 bang:
  - `iceberg.gold.covid_pathway_summary`
  - `iceberg.gold.mortality_by_demographics`
- Ten bang `revenue_analysis` va `covid_trends` chua ton tai trong schema `iceberg.gold` o lan kiem tra nay.

## 2. Kiem dinh du lieu Gold voi nguon Silver/Bronze

### 2.1. Kiem dinh bang pathway summary

Da tinh lai toan bo chi so tu nguon:

- `iceberg.silver.covid_clinical_master`
- `iceberg.bronze.encounters`
- `iceberg.bronze.conditions`

Ket qua doi chieu:

- total_covid_cases: `88166` = `88166`
- home_isolation_cases: `61188` = `61188`
- hospitalized_cases: `26978` = `26978`
- icu_cases: `0` = `0`
- ventilation_cases: `18175` = `18175`
- recovered_cases: `84525` = `84525`
- death_cases: `3641` = `3641`

### 2.2. Kiem dinh bang mortality by demographics

Da tinh lai tu `iceberg.silver.covid_clinical_master` theo logic age_group + gender.
Tat ca nhom deu match 100% voi Gold (10/10 nhom), bao gom:

- total_covid_patients
- total_deaths
- mortality_rate_percent

## 3. Kiem tra Iceberg Time-travel tren Trino

Da test thanh cong voi `FOR TIMESTAMP AS OF`:

- Query hien tai: `count(*) = 1`
- Query lui 1 phut: `count(*) = 1`

Luu y: Day la bang summary 1 dong, nen count giong nhau la binh thuong. Gia tri nghiep vu chi thay doi khi co snapshot moi.

## 4. SQL templates ban giao (cho Dashboard va API)

## 4.1. Template cho bang yeu cau de bai (neu team tao `revenue_analysis`, `covid_trends`)

```sql
-- A) Data quality checks cho revenue_analysis
SELECT
  COUNT(*) AS row_count,
  COUNT_IF(total_revenue < 0) AS negative_revenue_rows,
  COUNT_IF(revenue_date IS NULL) AS null_revenue_date_rows,
  MIN(revenue_date) AS min_date,
  MAX(revenue_date) AS max_date
FROM iceberg.gold.revenue_analysis;

-- B) Aggregation check (doi so tong theo thang)
SELECT
  date_trunc('month', revenue_date) AS month,
  SUM(total_revenue) AS monthly_revenue
FROM iceberg.gold.revenue_analysis
GROUP BY 1
ORDER BY 1;

-- C) Trend quality cho covid_trends
SELECT
  trend_date,
  new_cases,
  rolling_7d_cases,
  LAG(rolling_7d_cases) OVER (ORDER BY trend_date) AS prev_rolling_7d,
  rolling_7d_cases - LAG(rolling_7d_cases) OVER (ORDER BY trend_date) AS delta_rolling_7d
FROM iceberg.gold.covid_trends
ORDER BY trend_date;

-- D) Outlier scan
SELECT *
FROM iceberg.gold.covid_trends
WHERE new_cases < 0 OR rolling_7d_cases < 0
ORDER BY trend_date;
```

## 4.2. Template phu hop voi bang Gold hien tai trong repo

```sql
-- A) Kiem tra thong ke tong hop pathway
SELECT *
FROM iceberg.gold.covid_pathway_summary;

-- B) So khop tong tu Silver/Bronze voi Gold pathway
WITH per_patient AS (
  SELECT
    s.patient_id,
    s.patient_deathdate,
    MAX(CASE WHEN lower(e.encounterclass) LIKE '%inpatient%' THEN 1 ELSE 0 END) AS has_hosp,
    MAX(CASE WHEN lower(e.encounterclass) LIKE '%icu%' OR lower(e.encounterclass) LIKE '%intensive%' THEN 1 ELSE 0 END) AS has_icu,
    MAX(CASE WHEN lower(c.description) LIKE '%ventilator%' OR lower(c.description) LIKE '%ventilation%' OR lower(c.description) LIKE '%hypoxemia%' THEN 1 ELSE 0 END) AS has_vent
  FROM iceberg.silver.covid_clinical_master s
  LEFT JOIN iceberg.bronze.encounters e ON e.patient = s.patient_id
  LEFT JOIN iceberg.bronze.conditions c ON c.patient = s.patient_id
  GROUP BY s.patient_id, s.patient_deathdate
),
calc AS (
  SELECT
    COUNT(*) AS total_covid_cases,
    SUM(CASE WHEN has_hosp = 0 THEN 1 ELSE 0 END) AS home_isolation_cases,
    SUM(has_hosp) AS hospitalized_cases,
    SUM(has_icu) AS icu_cases,
    SUM(has_vent) AS ventilation_cases,
    SUM(CASE WHEN patient_deathdate IS NULL THEN 1 ELSE 0 END) AS recovered_cases,
    SUM(CASE WHEN patient_deathdate IS NOT NULL THEN 1 ELSE 0 END) AS death_cases
  FROM per_patient
)
SELECT
  c.*, g.*
FROM calc c
CROSS JOIN iceberg.gold.covid_pathway_summary g;

-- C) Kiem tra mortality by demographics
WITH calc AS (
  SELECT
    CASE
      WHEN age <= 18 THEN '0-18'
      WHEN age <= 34 THEN '19-34'
      WHEN age <= 49 THEN '35-49'
      WHEN age <= 64 THEN '50-64'
      WHEN age >= 65 THEN '65+'
      ELSE 'Unknown'
    END AS age_group,
    gender,
    COUNT(*) AS total_covid_patients,
    SUM(CASE WHEN patient_deathdate IS NOT NULL THEN 1 ELSE 0 END) AS total_deaths,
    ROUND(100.0 * SUM(CASE WHEN patient_deathdate IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS mortality_rate_percent
  FROM (
    SELECT
      patient_id,
      gender,
      patient_deathdate,
      CASE
        WHEN patient_deathdate IS NOT NULL THEN year(patient_deathdate) - year(patient_birthdate)
        ELSE year(current_date) - year(patient_birthdate)
      END AS age
    FROM iceberg.silver.covid_clinical_master
  ) t
  GROUP BY 1, 2
)
SELECT
  COALESCE(c.age_group, g.age_group) AS age_group,
  COALESCE(c.gender, g.gender) AS gender,
  c.total_covid_patients AS rec_total,
  g.total_covid_patients AS gold_total,
  c.total_deaths AS rec_deaths,
  g.total_deaths AS gold_deaths,
  c.mortality_rate_percent AS rec_rate,
  g.mortality_rate_percent AS gold_rate
FROM calc c
FULL OUTER JOIN iceberg.gold.mortality_by_demographics g
  ON c.age_group = g.age_group AND c.gender = g.gender
ORDER BY 1, 2;

-- D) Time-travel test
SELECT count(*) AS current_count
FROM iceberg.gold.covid_pathway_summary;

SELECT count(*) AS count_1m_ago
FROM iceberg.gold.covid_pathway_summary
FOR TIMESTAMP AS OF (current_timestamp - INTERVAL '1' MINUTE);
```

## 5. Ket luan Task 3.2

- Kiem dinh du lieu Gold thanh cong tren bo bang Gold hien co trong Trino.
- Logic tong hop khop voi nguon Silver/Bronze o cac chi so da doi chieu.
- Time-travel Iceberg qua Trino hoat dong.
- SQL templates da san sang de ban giao cho team Dashboard/API.
