# Time-Series Data Ingestion Pipeline Documentation

## Overview

This pipeline provides a complete solution for ingesting, storing, and analyzing time-series power consumption data with weather correlations. The system is optimized for time-series queries with partitioning, indexing, and automated feature engineering.

## Architecture

### Components

1. **Database Schema (`db_schema.py`)**: Creates optimized tables with partitioning
2. **Ingestion Pipeline (`ingestion_pipeline.py`)**: Loads data from CSV or Azure Blob Storage
3. **Feature Engineering (`feature_engineering.py`)**: Computes analytical features
4. **Batch Pipeline (`batch_pipeline.py`)**: Automated end-to-end pipeline

## Database Schema

### Tables

#### 1. `power_consumption_hourly`
Main time-series table storing hourly power consumption and weather data.

**Partitioning**: Monthly partitioning on `year_month` computed column for query performance.

**Key Columns**:
- `datetime_utc`: Primary time dimension (indexed)
- `global_active_power`: Main power consumption metric
- Weather metrics: `temperature`, `humidity`, `pressure`, `wind_speed`, `precipitation`
- Engineered features: `hour_of_day`, `day_of_week`, `month_of_year`, `is_weekend`
- Lag features: `lag_power_1h`, `lag_power_24h`, `lag_temp_1h`

**Indexes**:
- `IX_power_consumption_datetime`: On `datetime_utc` for time-range queries
- `IX_power_consumption_year_month`: On `year_month` for partition pruning
- `IX_power_consumption_hour_day`: On `hour_of_day, day_of_week` for pattern analysis

#### 2. `power_consumption_daily`
Daily aggregated features for faster analytical queries.

**Key Aggregations**:
- Power: `total_active_power`, `avg_active_power`, `max_active_power`, `min_active_power`, `std_active_power`
- Weather: `avg_temperature`, `max_temperature`, `min_temperature`, `avg_humidity`, `total_precipitation`
- Sub-metering totals: `total_sub_metering_1`, `total_sub_metering_2`, `total_sub_metering_3`

**Indexes**:
- `IX_daily_date`: On `date_utc` for date lookups
- `IX_daily_month_weekend`: On `month_of_year, is_weekend` for seasonal analysis

#### 3. `peak_load_stats`
Peak load statistics for different aggregation periods.

**Key Metrics**:
- `peak_load_value`: Maximum load for the period
- `peak_load_datetime`: When peak load occurred
- `peak_load_hour`: Hour of peak load
- `avg_load`, `min_load`: Average and minimum loads
- `peak_to_avg_ratio`: Peak to average ratio
- `load_factor`: Average load / peak load (utilization metric)

**Aggregation Periods**: `hourly`, `daily`, `monthly`

#### 4. `weather_correlations`
Weather-energy correlation metrics.

**Correlations** (Pearson correlation coefficients):
- `temp_power_correlation`: Temperature vs power consumption
- `humidity_power_correlation`: Humidity vs power consumption
- `pressure_power_correlation`: Pressure vs power consumption
- `wind_speed_power_correlation`: Wind speed vs power consumption
- `precipitation_power_correlation`: Precipitation vs power consumption

**Regression Metrics** (for temperature):
- `temp_power_slope`: Linear regression slope
- `temp_power_intercept`: Linear regression intercept
- `temp_power_r_squared`: R-squared value

## Data Flow

```
CSV/Azure Blob → Ingestion Pipeline → power_consumption_hourly
                                           ↓
                              Feature Engineering
                                           ↓
                    ┌──────────────────────┼──────────────────────┐
                    ↓                      ↓                      ↓
        power_consumption_daily    peak_load_stats    weather_correlations
```

## Usage

### 1. Create Database Schema

```bash
python -c "from db_schema import create_time_series_schema; \
          create_time_series_schema('YOUR_CONNECTION_STRING')"
```

Or use the batch pipeline with `--create-schema` flag.

### 2. Ingest Data from CSV

```bash
python batch_pipeline.py \
    --data-source processed_power_and_weather.csv \
    --source-type csv \
    --create-schema \
    --sql-connection "YOUR_CONNECTION_STRING"
```

### 3. Ingest Data from Azure Blob Storage

```bash
python batch_pipeline.py \
    --data-source processed/processed_power_and_weather.csv \
    --source-type azure \
    --azure-connection "YOUR_AZURE_CONNECTION_STRING" \
    --container-name "greenpowerstorage-container" \
    --create-schema \
    --sql-connection "YOUR_CONNECTION_STRING"
```

### 4. Run Feature Engineering Only

```python
from feature_engineering import run_all_feature_engineering

results = run_all_feature_engineering(
    connection_string="YOUR_CONNECTION_STRING",
    start_date=datetime(2007, 1, 1),
    end_date=datetime(2007, 12, 31)
)
```

### 5. Automated Nightly Batch (Cron/Windows Task Scheduler)

**Linux/Mac (Cron)**:
```bash
# Run daily at 2 AM
0 2 * * * cd /path/to/project && python batch_pipeline.py \
    --data-source processed/processed_power_and_weather.csv \
    --source-type azure \
    --sql-connection "$SQL_CONNECTION_STRING" \
    --azure-connection "$AZURE_CONNECTION_STRING" \
    --container-name "greenpowerstorage-container" \
    >> /var/log/pipeline.log 2>&1
```

**Windows Task Scheduler**:
- Create a scheduled task that runs `batch_pipeline.py` with appropriate arguments
- Set trigger to daily at 2 AM
- Configure environment variables for connection strings

## Feature Engineering Transformations

### 1. Daily Aggregates

**Transformation**: Aggregates hourly data to daily level

**Metrics Computed**:
- **Power Consumption**:
  - Total, average, max, min, standard deviation of active power
  - Total and average reactive power
  - Average voltage and intensity
  - Total sub-metering values (kitchen, laundry, HVAC)

- **Weather**:
  - Average, max, min temperature
  - Average humidity
  - Total precipitation
  - Average wind speed and pressure

**Use Cases**:
- Daily consumption reports
- Seasonal trend analysis
- Weekend vs weekday comparisons

### 2. Peak Load Statistics

**Transformation**: Identifies peak load periods and computes load metrics

**Metrics Computed**:
- Peak load value and timestamp
- Peak load hour
- Average and minimum loads
- Load variance
- Peak-to-average ratio
- Load factor (utilization)

**Aggregation Periods**:
- **Hourly**: Peak load per hour
- **Daily**: Peak load per day
- **Monthly**: Peak load per month

**Use Cases**:
- Demand forecasting
- Capacity planning
- Load balancing
- Peak shaving strategies

### 3. Weather Correlations

**Transformation**: Computes correlation between weather variables and power consumption

**Correlation Methods**:
- **Pearson Correlation**: Linear relationship strength
- **Linear Regression**: Temperature-power relationship modeling

**Weather Variables Analyzed**:
- Temperature (with regression metrics)
- Relative humidity
- Atmospheric pressure
- Wind speed
- Precipitation

**Use Cases**:
- Understanding weather impact on consumption
- Predictive modeling features
- Energy efficiency analysis
- HVAC system optimization

## Performance Optimizations

### 1. Partitioning
- **Monthly partitioning** on `year_month` column
- Enables partition pruning for time-range queries
- Improves query performance for large datasets

### 2. Indexing
- **Time-based indexes**: Fast time-range queries
- **Composite indexes**: Optimized for common query patterns
- **Partition-aware indexes**: Aligned with partitioning strategy

### 3. Batch Processing
- Configurable batch size (default: 1000 rows)
- Reduces memory usage
- Enables progress tracking
- Error isolation per batch

### 4. Upsert Mode
- Prevents duplicate data
- Updates existing records
- Handles incremental loads

## Query Examples

### Get Daily Consumption for a Month

```sql
SELECT 
    date_utc,
    total_active_power,
    avg_active_power,
    max_active_power
FROM power_consumption_daily
WHERE date_utc >= '2007-01-01' AND date_utc < '2007-02-01'
ORDER BY date_utc;
```

### Find Peak Load Days

```sql
SELECT 
    date_utc,
    peak_load_value,
    peak_load_hour,
    peak_to_avg_ratio
FROM peak_load_stats
WHERE aggregation_period = 'daily'
ORDER BY peak_load_value DESC
LIMIT 10;
```

### Analyze Weather-Power Correlation

```sql
SELECT 
    date_utc,
    temp_power_correlation,
    temp_power_r_squared,
    humidity_power_correlation
FROM weather_correlations
WHERE aggregation_period = 'daily'
  AND temp_power_correlation IS NOT NULL
ORDER BY ABS(temp_power_correlation) DESC;
```

### Hourly Pattern Analysis

```sql
SELECT 
    hour_of_day,
    AVG(global_active_power) as avg_power,
    MAX(global_active_power) as max_power,
    COUNT(*) as sample_count
FROM power_consumption_hourly
WHERE datetime_utc >= '2007-01-01' AND datetime_utc < '2008-01-01'
GROUP BY hour_of_day
ORDER BY hour_of_day;
```

## Error Handling

- **Batch-level errors**: Logged and skipped, processing continues
- **Row-level errors**: Logged with row details, skipped
- **Connection errors**: Retried with timeout
- **Schema errors**: Detailed error messages with suggestions

## Monitoring

### Logging
- All operations logged with timestamps
- Error details with stack traces
- Progress indicators for long-running operations
- Summary statistics after completion

### Metrics to Monitor
- Rows ingested per batch
- Feature engineering record counts
- Query performance (execution time)
- Partition sizes
- Index usage statistics

## Troubleshooting

### Common Issues

1. **Partitioning Errors**: If partitioning fails, falls back to non-partitioned table
2. **Connection Timeouts**: Increase timeout in connection string
3. **Memory Issues**: Reduce batch size
4. **Duplicate Key Errors**: Use upsert mode instead of insert

### Debug Mode

Run with `--verbose` flag for detailed logging:

```bash
python batch_pipeline.py --data-source data.csv --verbose
```

## Future Enhancements

- [ ] Streaming ingestion support (Kafka, Event Hubs)
- [ ] Real-time feature computation
- [ ] Automated anomaly detection
- [ ] ML model integration
- [ ] TimescaleDB support
- [ ] Data retention policies
- [ ] Automated partitioning maintenance

## Dependencies

See `requirements.txt` for complete list:
- pandas: Data manipulation
- pyodbc: SQL Server connectivity
- numpy/scipy: Statistical computations
- azure-storage-blob: Azure Blob Storage access

## Support

For issues or questions, refer to:
- Database schema: `db_schema.py`
- Ingestion logic: `ingestion_pipeline.py`
- Feature engineering: `feature_engineering.py`
- Batch pipeline: `batch_pipeline.py`

