# ==============================================================================
# Healthcare Lakehouse: COVID-19 Analytics - EXECUTION SCRIPT (PowerShell)
# ==============================================================================
# This script automates the entire pipeline:
# 1. Download & Prepare Data
# 2. Start Infrastructure
# 3. Execute Medallion ETL (Bronze -> Silver -> Gold)
# 4. Display Query Evidence
# ==============================================================================

$ErrorActionPreference = "Stop"

# --- Configuration ---
$COMPOSE_FILE = "deploy/docker-compose.yml"
$DATA_DIR = "data"
$DATA_URL = "https://mitre.box.com/shared/static/wk3560f962ozlg7sd2oj1zxk73ayqvm0.zip"
$ENV_FILE = ".env"

Write-Host "----------------------------------------------------------------" -ForegroundColor Cyan
Write-Host "🏥 HEALTHCARE LAKEHOUSE: COVID-19 PIPELINE EXECUTION" -ForegroundColor Cyan
Write-Host "----------------------------------------------------------------" -ForegroundColor Cyan

# 1. Prepare Data
if (-not (Test-Path "$DATA_DIR/patients.csv") -or -not (Test-Path "$DATA_DIR/encounters.csv") -or -not (Test-Path "$DATA_DIR/conditions.csv")) {
    Write-Host "[STEP 1/6] Data files missing. Downloading from MITRE..."
    if (-not (Test-Path $DATA_DIR)) { New-Item -ItemType Directory -Path $DATA_DIR }
    
    $zipPath = "$DATA_DIR/synthea_data.zip"
    Invoke-WebRequest -Uri $DATA_URL -OutFile $zipPath
    
    Write-Host "[INFO] Unzipping data..."
    Expand-Archive -Path $zipPath -DestinationPath $DATA_DIR -Force
    Remove-Item $zipPath
    Write-Host "[OK] Data ready in $DATA_DIR/"
} else {
    Write-Host "[STEP 1/6] Data files already exist. Skipping download."
}

# 2. Check Environment
if (-not (Test-Path $ENV_FILE)) {
    Write-Host "[ERROR] .env file not found! Please create it before running." -ForegroundColor Red
    exit 1
}

# 3. Start Infrastructure
Write-Host "[STEP 2/6] Starting background infrastructure (MinIO, Trino, Postgres)..."
docker compose --env-file $ENV_FILE -f $COMPOSE_FILE up -d

# 4. Wait for Health
Write-Host "[STEP 3/6] Waiting for Trino and Postgres to be healthy..."
while ($true) {
    try {
        $check = docker exec trino trino --execute "SELECT 1" 2>$null
        if ($check -match "1") { break }
    } catch {}
    Write-Host -NoNewline "."
    Start-Sleep -Seconds 5
}
Write-Host ""
Write-Host "[OK] Infrastructure is ONLINE." -ForegroundColor Green

# 5. Execute ETL Pipeline
Write-Host "[STEP 4/6] Executing Medallion Pipeline..." -ForegroundColor Yellow

Write-Host ">> Running RAW Ingestion (Postgres -> MinIO Raw)..."
docker compose --env-file $ENV_FILE -f $COMPOSE_FILE up --force-recreate spark-raw-job
docker logs spark-raw-job --tail 5

Write-Host ">> Running BRONZE Ingestion (Raw -> Iceberg Bronze)..."
docker compose --env-file $ENV_FILE -f $COMPOSE_FILE up --force-recreate spark-bronze-job
docker logs spark-bronze-job --tail 5

Write-Host ">> Running SILVER Cleansing (Bronze -> Iceberg Silver)..."
docker compose --env-file $ENV_FILE -f $COMPOSE_FILE up --force-recreate spark-silver-job
docker logs spark-silver-job --tail 5

Write-Host ">> Running GOLD Aggregation (Silver -> Iceberg Gold)..."
docker compose --env-file $ENV_FILE -f $COMPOSE_FILE up --force-recreate spark-gold-job
docker logs spark-gold-job --tail 5

# 6. Query Evidence
Write-Host ""
Write-Host "----------------------------------------------------------------" -ForegroundColor Cyan
Write-Host "📊 PIPELINE COMPLETE - QUERY EVIDENCE (TRINO)" -ForegroundColor Cyan
Write-Host "----------------------------------------------------------------" -ForegroundColor Cyan

Write-Host "[QUERY 1] Symptoms Outcomes Summary (Gold):"
docker exec trino trino --execute "SELECT condition_category, is_survivor, patient_count, rate_percentage FROM hospital.gold.symptoms_outcomes ORDER BY condition_category, is_survivor" --output-format ALIGNED

Write-Host "[QUERY 2] Mortality Demographics (Gold):"
docker exec trino trino --execute "SELECT age_range, gender, mortality_count FROM hospital.gold.mortality_demographics ORDER BY mortality_count DESC LIMIT 5" --output-format ALIGNED

Write-Host "[QUERY 3] Hospitalization Workload (Gold):"
docker exec trino trino --execute "SELECT encounter_description, patient_count, avg_length_of_stay_days, percentage FROM hospital.gold.hospitalization_workload" --output-format ALIGNED

Write-Host "[QUERY 4] Data Volume Verification:"
docker exec trino trino --execute "SELECT 'Bronze Conditions' as Layer, count(*) as Records FROM hospital.bronze.conditions UNION ALL SELECT 'Silver COVID Master' as Layer, count(*) as Records FROM hospital.silver.covid_clinical_master" --output-format ALIGNED

Write-Host "----------------------------------------------------------------" -ForegroundColor Cyan
Write-Host "✅ ALL STEPS COMPLETED SUCCESSFULLY." -ForegroundColor Green
Write-Host "----------------------------------------------------------------" -ForegroundColor Cyan
