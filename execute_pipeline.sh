#!/bin/bash

# ==============================================================================
# Healthcare Lakehouse: COVID-19 Analytics - EXECUTION SCRIPT
# ==============================================================================
# This script automates the entire pipeline:
# 1. Download & Prepare Data
# 2. Start Infrastructure
# 3. Execute Medallion ETL (Bronze -> Silver -> Gold)
# 4. Display Query Evidence
# ==============================================================================

set -e

# --- Configuration ---
COMPOSE_FILE="deploy/docker-compose.yml"
DATA_DIR="data"
DATA_URL="https://mitre.box.com/shared/static/wk3560f962ozlg7sd2oj1zxk73ayqvm0.zip"
ENV_FILE=".env"

echo "----------------------------------------------------------------"
echo "🏥 HEALTHCARE LAKEHOUSE: COVID-19 PIPELINE EXECUTION"
echo "----------------------------------------------------------------"

# 1. Prepare Data
if [ ! -f "$DATA_DIR/patients.csv" ] || [ ! -f "$DATA_DIR/encounters.csv" ] || [ ! -f "$DATA_DIR/conditions.csv" ]; then
    echo "[STEP 1/6] Data files missing. Downloading from MITRE..."
    mkdir -p "$DATA_DIR"
    curl -L "$DATA_URL" -o "$DATA_DIR/synthea_data.zip"
    echo "[INFO] Unzipping data..."
    unzip -o "$DATA_DIR/synthea_data.zip" -d "$DATA_DIR"
    rm "$DATA_DIR/synthea_data.zip"
    echo "[OK] Data ready in $DATA_DIR/"
else
    echo "[STEP 1/6] Data files already exist. Skipping download."
fi

# 2. Check Environment
if [ ! -f "$ENV_FILE" ]; then
    echo "[ERROR] .env file not found! Please create it before running."
    exit 1
fi

# 3. Start Infrastructure
echo "[STEP 2/6] Starting background infrastructure (MinIO, Trino, Postgres)..."
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up -d

# 4. Wait for Health
echo "[STEP 3/6] Waiting for Trino and Postgres to be healthy..."
until docker exec trino trino --execute "SELECT 1" > /dev/null 2>&1; do
    echo -n "."
    sleep 5
done
echo -e "\n[OK] Infrastructure is ONLINE."

# 5. Execute ETL Pipeline
echo "[STEP 4/6] Executing Medallion Pipeline..."

echo ">> Running RAW Ingestion (Postgres -> MinIO Raw)..."
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up --force-recreate spark-raw-job
docker logs spark-raw-job | tail -n 5

echo ">> Running BRONZE Ingestion (Raw -> Iceberg Bronze)..."
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up --force-recreate spark-bronze-job
docker logs spark-bronze-job | tail -n 5

echo ">> Running SILVER Cleansing (Bronze -> Iceberg Silver)..."
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up --force-recreate spark-silver-job
docker logs spark-silver-job | tail -n 5

echo ">> Running GOLD Aggregation (Silver -> Iceberg Gold)..."
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up --force-recreate spark-gold-job
docker logs spark-gold-job | tail -n 5

# 6. Query Evidence
echo ""
echo "----------------------------------------------------------------"
echo "📊 PIPELINE COMPLETE - QUERY EVIDENCE (TRINO)"
echo "----------------------------------------------------------------"

echo "[QUERY 1] Symptoms Outcomes Summary (Gold):"
docker exec trino trino --execute "SELECT condition_category, is_survivor, patient_count, rate_percentage FROM hospital.gold.symptoms_outcomes ORDER BY condition_category, is_survivor" --output-format ALIGNED

echo "[QUERY 2] Mortality Demographics (Gold):"
docker exec trino trino --execute "SELECT age_range, gender, mortality_count FROM hospital.gold.mortality_demographics ORDER BY mortality_count DESC LIMIT 5" --output-format ALIGNED

echo "[QUERY 3] Hospitalization Workload (Gold):"
docker exec trino trino --execute "SELECT encounter_description, patient_count, avg_length_of_stay_days, percentage FROM hospital.gold.hospitalization_workload" --output-format ALIGNED

echo "[QUERY 4] Data Volume Verification:"
docker exec trino trino --execute "
    SELECT 'Bronze Conditions' as Layer, count(*) as Records FROM hospital.bronze.conditions
    UNION ALL
    SELECT 'Silver COVID Master' as Layer, count(*) as Records FROM hospital.silver.covid_clinical_master
" --output-format ALIGNED

echo "----------------------------------------------------------------"
echo "✅ ALL STEPS COMPLETED SUCCESSFULLY."
echo "----------------------------------------------------------------"
