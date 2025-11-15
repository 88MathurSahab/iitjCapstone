# IF OBJECT_ID('peak_load_stats_hourly', 'U') IS NOT NULL
#   DROP TABLE peak_load_stats_hourly;

# CREATE TABLE peak_load_stats_hourly (
#     id BIGINT IDENTITY(1,1) PRIMARY KEY,
#     date_utc DATE NOT NULL,
#     aggregation_period VARCHAR(20) NOT NULL, -- 'hourly'
    
#     peak_load_value FLOAT NOT NULL,
#     peak_load_datetime DATETIME2 NOT NULL,
#     peak_load_hour TINYINT,
    
#     avg_load FLOAT NOT NULL,
#     min_load FLOAT NOT NULL,
#     load_variance FLOAT,
    
#     peak_to_avg_ratio FLOAT,
#     load_factor FLOAT, -- avg_load / peak_load
    
#     created_at DATETIME2 DEFAULT GETUTCDATE(),

#     -- Uniqueness: one row per hour bucket
#     CONSTRAINT UQ_peak_load_stats_hourly UNIQUE (date_utc, aggregation_period, peak_load_datetime)
# );

# ==================================================

# WITH Base AS (
#   SELECT
#     CAST(datetime_utc AS DATE) AS date_utc,
#     DATEPART(HOUR, datetime_utc) AS hour_of_day,
#     datetime_utc,
#     global_active_power,
#     CAST(global_active_power AS FLOAT) AS gap
#   FROM power_consumption_hourly
#   WHERE datetime_utc IS NOT NULL
# ),
# Ranked AS (
#   SELECT
#     date_utc,
#     hour_of_day,
#     datetime_utc,
#     global_active_power,
#     AVG(gap) OVER (ORDER BY date_utc ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS avg_load_3h,
#     VARP(gap) OVER (ORDER BY date_utc ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS load_variance_3h,
#     ROW_NUMBER() OVER (
#       PARTITION BY date_utc, hour_of_day
#       ORDER BY global_active_power DESC, datetime_utc DESC
#     ) AS rn
#   FROM Base
# ),
# PeakPerHour AS (
#   SELECT
#     date_utc,
#     hour_of_day,
#     datetime_utc AS peak_load_datetime,
#     global_active_power AS peak_load_value
#   FROM Ranked
#   WHERE rn = 1
# ),
# AggPerHour AS (
#   SELECT
#     date_utc,
#     hour_of_day,
#     AVG(global_active_power) AS avg_load,
#     MIN(global_active_power) AS min_load,
#     CASE WHEN COUNT(CASE WHEN global_active_power IS NOT NULL THEN 1 END) >= 2
#          THEN VARP(global_active_power) ELSE 0.0 END AS load_variance
#   FROM Base
#   GROUP BY date_utc, hour_of_day
# )

# INSERT INTO peak_load_stats_hourly (
#   date_utc, aggregation_period, peak_load_value, peak_load_datetime, peak_load_hour,
#   avg_load, min_load, load_variance, peak_to_avg_ratio, load_factor
# )
# SELECT
#   p.date_utc,
#   'hourly',
#   p.peak_load_value,
#   p.peak_load_datetime,
#   CAST(p.hour_of_day AS TINYINT),
#   a.avg_load,
#   a.min_load,
#   a.load_variance,
#   p.peak_load_value / NULLIF(a.avg_load, 0),
#   a.avg_load / NULLIF(p.peak_load_value, 0)
# FROM PeakPerHour p
# JOIN AggPerHour a
#   ON a.date_utc = p.date_utc AND a.hour_of_day = p.hour_of_day;

# CREATE INDEX IX_peak_load_stats_hourly_date ON peak_load_stats_hourly(date_utc, aggregation_period);
# CREATE INDEX IX_peak_load_stats_hourly_dt ON peak_load_stats_hourly(peak_load_datetime);

