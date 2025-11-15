# How to Run the Pipeline - Terminal and Docker Guide

## Prerequisites

### For Terminal (Local Linux/Mac)

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

### For Docker

Docker setup is already configured in the Dockerfile - no additional setup needed!

---

## Running from Terminal

### Step 1: Set Environment Variables (Optional)

```bash
export SQL_CONNECTION_STRING="DRIVER={ODBC Driver 18 for SQL Server};SERVER=tcp:greenpowerutilities-dbserver.database.windows.net,1433;DATABASE=GreenPowerUtilities_DB;Uid=CloudSAe9b21290;Pwd=Yash@8896;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

export AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=greenpowerstorage;AccountKey=GNvSIF/x7DFrDBBDiuyRVBRjGBs3J41+TNnUBjiiO0Pj/BImCvK7Bp5fCnzmYw+RV5ucKtrT17D4+AStt6eZ5Q==;EndpointSuffix=core.windows.net"

export AZURE_CONTAINER_NAME="greenpowerstorage-container"
```

### Step 2: Run the Pipeline

**Option A: Ingest from local CSV file**

```bash
python batch_pipeline.py \
    --data-source processed_power_and_weather.csv \
    --source-type csv \
    --create-schema \
    --sql-connection "$SQL_CONNECTION_STRING"
```

**Option B: Ingest from Azure Blob Storage**

```bash
python batch_pipeline.py \
    --data-source processed/processed_power_and_weather.csv \
    --source-type azure \
    --azure-connection "$AZURE_CONNECTION_STRING" \
    --container-name "$AZURE_CONTAINER_NAME" \
    --create-schema \
    --sql-connection "$SQL_CONNECTION_STRING"
```

**Option C: Skip schema creation (if already exists)**

```bash
python batch_pipeline.py \
    --data-source processed_power_and_weather.csv \
    --source-type csv \
    --sql-connection "$SQL_CONNECTION_STRING"
```

**Option D: Skip feature engineering (faster, just ingest data)**

```bash
python batch_pipeline.py \
    --data-source processed_power_and_weather.csv \
    --source-type csv \
    --create-schema \
    --skip-features \
    --sql-connection "$SQL_CONNECTION_STRING"
```

### Step 3: Verify Data

```bash
# Check if data was ingested (using Python)
python -c "
import pyodbc
import os
conn = pyodbc.connect(os.getenv('SQL_CONNECTION_STRING'))
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM power_consumption_hourly')
print(f'Hourly records: {cursor.fetchone()[0]}')
cursor.execute('SELECT COUNT(*) FROM power_consumption_daily')
print(f'Daily records: {cursor.fetchone()[0]}')
conn.close()
"
```

---

## Running with Docker

### Step 1: Build the Docker Image

```bash
docker build -t iitj-capstone .
```

### Step 2: Run the Pipeline

**Option A: Run batch pipeline with local CSV (mount file)**

```bash
docker run --rm \
    -v $(pwd)/processed_power_and_weather.csv:/app/processed_power_and_weather.csv \
    -e SQL_CONNECTION_STRING="DRIVER={ODBC Driver 18 for SQL Server};SERVER=tcp:greenpowerutilities-dbserver.database.windows.net,1433;DATABASE=GreenPowerUtilities_DB;Uid=CloudSAe9b21290;Pwd=Yash@8896;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;" \
    iitj-capstone \
    python batch_pipeline.py \
        --data-source processed_power_and_weather.csv \
        --source-type csv \
        --create-schema
```

**Option B: Run batch pipeline from Azure Blob**

```bash
docker run --rm \
    -e SQL_CONNECTION_STRING="DRIVER={ODBC Driver 18 for SQL Server};SERVER=tcp:greenpowerutilities-dbserver.database.windows.net,1433;DATABASE=GreenPowerUtilities_DB;Uid=CloudSAe9b21290;Pwd=Yash@8896;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;" \
    -e AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=greenpowerstorage;AccountKey=GNvSIF/x7DFrDBBDiuyRVBRjGBs3J41+TNnUBjiiO0Pj/BImCvK7Bp5fCnzmYw+RV5ucKtrT17D4+AStt6eZ5Q==;EndpointSuffix=core.windows.net" \
    -e AZURE_CONTAINER_NAME="greenpowerstorage-container" \
    iitj-capstone \
    python batch_pipeline.py \
        --data-source processed/processed_power_and_weather.csv \
        --source-type azure \
        --create-schema
```

**Option C: Run original apiTest.py (data processing)**

```bash
docker run --rm \
    -e SQL_CONNECTION_STRING="..." \
    -e AZURE_CONNECTION_STRING="..." \
    iitj-capstone \
    python -m apiTest
```

### Step 3: Using Docker Compose (Optional)

Create `docker-compose.yml`:

```yaml
version: "3.8"

services:
  pipeline:
    build: .
    environment:
      - SQL_CONNECTION_STRING=${SQL_CONNECTION_STRING}
      - AZURE_CONNECTION_STRING=${AZURE_CONNECTION_STRING}
      - AZURE_CONTAINER_NAME=greenpowerstorage-container
    volumes:
      - ./processed_power_and_weather.csv:/app/processed_power_and_weather.csv
    command: python batch_pipeline.py --data-source processed_power_and_weather.csv --source-type csv --create-schema
```

Run with:

```bash
docker-compose up
```

---

## Common Commands Reference

### Terminal Commands

```bash

# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Run full pipeline (schema + ingestion + features)
python batch_pipeline.py --data-source processed_power_and_weather.csv --source-type csv --create-schema

# 3. Run with verbose logging
python batch_pipeline.py --data-source processed_power_and_weather.csv --source-type csv --create-schema --verbose

# 4. Run with custom batch size (for large files)
python batch_pipeline.py --data-source processed_power_and_weather.csv --source-type csv --batch-size 500

# 5. Run only feature engineering (if data already ingested)
python -c "from feature_engineering import run_all_feature_engineering; from apiTest import DEFAULT_SQL_CONNECTION_STRING; run_all_feature_engineering(DEFAULT_SQL_CONNECTION_STRING)"
```

### Docker Commands

```bash
# 1. Build image
docker build -t iitj-capstone .

# 2. Run pipeline
docker run --rm -e SQL_CONNECTION_STRING="..." iitj-capstone python batch_pipeline.py --data-source processed_power_and_weather.csv --source-type csv --create-schema

# 3. Run with interactive shell (for debugging)
docker run --rm -it -e SQL_CONNECTION_STRING="..." iitj-capstone /bin/bash

# 4. View logs
docker run --rm -e SQL_CONNECTION_STRING="..." iitj-capstone python batch_pipeline.py --data-source processed_power_and_weather.csv --source-type csv --create-schema --verbose

# 5. Run with volume mount for local files
docker run --rm -v $(pwd):/app/data -e SQL_CONNECTION_STRING="..." iitj-capstone python batch_pipeline.py --data-source /app/data/processed_power_and_weather.csv --source-type csv --create-schema
```

---

## Troubleshooting

### Issue: ODBC Driver Not Found

**Error:** `Can't open lib 'ODBC Driver 18 for SQL Server' : file not found`

```

### Issue: Connection Timeout

**Solution:** Increase timeout in connection string:
```

Connection Timeout=60;

````

### Issue: Docker Build Fails

**Solution:** Make sure you have internet connection and Microsoft repositories are accessible:
```bash
# Test repository access
curl https://packages.microsoft.com/keys/microsoft.asc
````

### Issue: Data Already Exists

**Solution:** Use upsert mode (default) or skip schema creation:

```bash
# Upsert mode (updates existing, inserts new) - DEFAULT
python batch_pipeline.py --data-source processed_power_and_weather.csv --source-type csv

# Or use insert-only (will fail on duplicates)
python batch_pipeline.py --data-source processed_power_and_weather.csv --source-type csv --insert-only
```

---

## Quick Start Checklist

- [ ] Install Python deps: `pip install -r requirements.txt`
- [ ] Set environment variables (optional)
- [ ] Run pipeline: `python batch_pipeline.py --data-source processed_power_and_weather.csv --source-type csv --create-schema`
- [ ] Verify: Check database for ingested records

---

## Automated Scheduling

### Cron (Linux/Mac)

Add to crontab (`crontab -e`):

```bash
# Run daily at 2 AM
0 2 * * * cd /path/to/project && python batch_pipeline.py --data-source processed/processed_power_and_weather.csv --source-type azure >> /var/log/pipeline.log 2>&1
```

### Docker Cron

Use a cron container or Kubernetes CronJob to run the Docker container on schedule.

---

## Next Steps

- Review `PIPELINE_DOCUMENTATION.md` for detailed architecture
- Check `QUICKSTART.md` for quick examples
- Explore feature engineering options in `feature_engineering.py`
