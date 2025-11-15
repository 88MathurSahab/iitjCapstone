# Quick Start Guide

## Setup

1. **Install Dependencies**:

```bash
pip install -r requirements.txt
```

2. **Set Environment Variables** (optional):

```bash
export SQL_CONNECTION_STRING="DRIVER={ODBC Driver 18 for SQL Server};SERVER=..."
export AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;..."
export AZURE_CONTAINER_NAME="greenpowerstorage-container"
```

## Basic Usage

### Step 1: Create Database Schema

```bash
python -c "from db_schema import create_time_series_schema; \
          from apiTest import DEFAULT_SQL_CONNECTION_STRING; \
          create_time_series_schema(DEFAULT_SQL_CONNECTION_STRING)"
```

### Step 2: Ingest Processed Data

```bash
python batch_pipeline.py \
    --data-source processed_power_and_weather.csv \
    --source-type csv \
    --create-schema
```

### Step 3: Verify Data

```python
import pyodbc
from apiTest import DEFAULT_SQL_CONNECTION_STRING

conn = pyodbc.connect(DEFAULT_SQL_CONNECTION_STRING)
cursor = conn.cursor()

# Check hourly data
cursor.execute("SELECT COUNT(*) FROM power_consumption_hourly")
print(f"Hourly records: {cursor.fetchone()[0]}")

# Check daily aggregates
cursor.execute("SELECT COUNT(*) FROM power_consumption_daily")
print(f"Daily records: {cursor.fetchone()[0]}")

# Check peak load stats
cursor.execute("SELECT COUNT(*) FROM peak_load_stats")
print(f"Peak load records: {cursor.fetchone()[0]}")

conn.close()
```

## Example Queries

### Get Daily Consumption Summary

```sql
SELECT TOP 10
    date_utc,
    total_active_power,
    avg_active_power,
    max_active_power,
    avg_temperature
FROM power_consumption_daily
ORDER BY date_utc DESC;
```

### Find Peak Load Days

```sql
SELECT TOP 10
    date_utc,
    peak_load_value,
    peak_load_hour,
    peak_to_avg_ratio
FROM peak_load_stats
WHERE aggregation_period = 'daily'
ORDER BY peak_load_value DESC;
```

### Weather-Power Correlation

```sql
SELECT
    AVG(temp_power_correlation) as avg_temp_correlation,
    AVG(humidity_power_correlation) as avg_humidity_correlation
FROM weather_correlations
WHERE aggregation_period = 'daily'
  AND temp_power_correlation IS NOT NULL;
```

## Automated Nightly Run

### Linux/Mac (Cron)

Add to crontab (`crontab -e`):

```
0 2 * * * cd /path/to/project && python batch_pipeline.py \
    --data-source processed/processed_power_and_weather.csv \
    --source-type azure \
    >> /var/log/pipeline.log 2>&1
```

### Windows Task Scheduler

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger: Daily at 2:00 AM
4. Action: Start a program
5. Program: `python`
6. Arguments: `batch_pipeline.py --data-source processed/processed_power_and_weather.csv --source-type azure`
7. Start in: `C:\path\to\project`

## Troubleshooting

### Connection Issues

- Verify connection string format
- Check firewall rules for Azure SQL Database
- Ensure ODBC driver is installed

### Partitioning Errors

- The system will fall back to non-partitioned table if partitioning fails
- Check SQL Server version (partitioning requires Enterprise or Standard edition)

### Memory Issues

- Reduce batch size: `--batch-size 500`
- Process data in smaller date ranges

### Duplicate Data

- Use upsert mode (default): `--upsert-mode` (already default)
- Or use insert-only: `--insert-only` (may fail on duplicates)

## Next Steps

- Review `PIPELINE_DOCUMENTATION.md` for detailed documentation
- Explore feature engineering options in `feature_engineering.py`
- Customize schema in `db_schema.py` for your needs
