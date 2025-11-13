"""
Feature Engineering Module
Creates analytical features: aggregations, peak load statistics, and weather correlations.
"""

import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
import pyodbc
from scipy import stats

logger = logging.getLogger(__name__)


def compute_daily_aggregates(
    connection_string: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> int:
    """
    Compute daily aggregated features from hourly data.
    
    Returns:
        Number of daily records created
    """
    logger.info("Computing daily aggregates")
    
    try:
        with pyodbc.connect(connection_string, timeout=120) as conn:
            cursor = conn.cursor()
            
            # Build date filter
            date_filter = ""
            params = []
            if start_date:
                date_filter += " AND datetime_utc >= ?"
                params.append(start_date)
            if end_date:
                date_filter += " AND datetime_utc <= ?"
                params.append(end_date)
            
            # Delete existing daily aggregates for the date range
            delete_query = f"""
                DELETE FROM power_consumption_daily
                WHERE date_utc >= CAST(? AS DATE) AND date_utc <= CAST(? AS DATE)
            """
            if start_date and end_date:
                cursor.execute(delete_query, (start_date.date(), end_date.date()))
                conn.commit()
            
            # Compute and insert daily aggregates
            insert_query = f"""
                INSERT INTO power_consumption_daily (
                    date_utc,
                    total_active_power,
                    avg_active_power,
                    max_active_power,
                    min_active_power,
                    std_active_power,
                    total_reactive_power,
                    avg_reactive_power,
                    avg_voltage,
                    avg_intensity,
                    total_sub_metering_1,
                    total_sub_metering_2,
                    total_sub_metering_3,
                    avg_temperature,
                    max_temperature,
                    min_temperature,
                    avg_humidity,
                    total_precipitation,
                    avg_wind_speed,
                    avg_pressure,
                    day_of_week,
                    month_of_year,
                    is_weekend
                )
                SELECT
                    CAST(datetime_utc AS DATE) as date_utc,
                    SUM(global_active_power) as total_active_power,
                    AVG(global_active_power) as avg_active_power,
                    MAX(global_active_power) as max_active_power,
                    MIN(global_active_power) as min_active_power,
                    STDEV(global_active_power) as std_active_power,
                    SUM(global_reactive_power) as total_reactive_power,
                    AVG(global_reactive_power) as avg_reactive_power,
                    AVG(voltage) as avg_voltage,
                    AVG(global_intensity) as avg_intensity,
                    SUM(sub_metering_1) as total_sub_metering_1,
                    SUM(sub_metering_2) as total_sub_metering_2,
                    SUM(sub_metering_3) as total_sub_metering_3,
                    AVG(temperature) as avg_temperature,
                    MAX(temperature) as max_temperature,
                    MIN(temperature) as min_temperature,
                    AVG(relative_humidity) as avg_humidity,
                    SUM(precipitation) as total_precipitation,
                    AVG(wind_speed) as avg_wind_speed,
                    AVG(pressure) as avg_pressure,
                    MAX(day_of_week) as day_of_week,
                    MAX(month_of_year) as month_of_year,
                    MAX(CAST(is_weekend AS INT)) as is_weekend
                FROM power_consumption_hourly
                WHERE datetime_utc IS NOT NULL {date_filter}
                GROUP BY CAST(datetime_utc AS DATE)
                ORDER BY date_utc
            """
            
            cursor.execute(insert_query, params)
            rows_affected = cursor.rowcount
            conn.commit()
            
            logger.info("Created %d daily aggregate records", rows_affected)
            return rows_affected
            
    except Exception as exc:
        logger.error("Failed to compute daily aggregates: %s", exc, exc_info=True)
        raise


def compute_peak_load_stats(
    connection_string: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    aggregation_period: str = "daily"
) -> int:
    """
    Compute peak load statistics.
    
    Args:
        connection_string: Database connection string
        start_date: Start date for computation
        end_date: End date for computation
        aggregation_period: 'hourly', 'daily', or 'monthly'
    
    Returns:
        Number of records created
    """
    logger.info("Computing peak load statistics for period: %s", aggregation_period)
    
    try:
        with pyodbc.connect(connection_string, timeout=120) as conn:
            cursor = conn.cursor()
            
            # Build date filter
            date_filter = ""
            params = []
            if start_date:
                date_filter += " AND datetime_utc >= ?"
                params.append(start_date)
            if end_date:
                date_filter += " AND datetime_utc <= ?"
                params.append(end_date)
            
            # Determine grouping based on aggregation period
            if aggregation_period == "hourly":
                group_by = "CAST(datetime_utc AS DATE), DATEPART(HOUR, datetime_utc)"
                date_expr = "CAST(datetime_utc AS DATE)"
            elif aggregation_period == "daily":
                group_by = "CAST(datetime_utc AS DATE)"
                date_expr = "CAST(datetime_utc AS DATE)"
            elif aggregation_period == "monthly":
                group_by = "YEAR(datetime_utc), MONTH(datetime_utc)"
                date_expr = "DATEFROMPARTS(YEAR(datetime_utc), MONTH(datetime_utc), 1)"
            else:
                raise ValueError(f"Unknown aggregation_period: {aggregation_period}")
            
            # Delete existing stats for the period
            delete_query = """
                DELETE FROM peak_load_stats
                WHERE aggregation_period = ? AND date_utc >= CAST(? AS DATE) AND date_utc <= CAST(? AS DATE)
            """
            if start_date and end_date:
                cursor.execute(delete_query, (aggregation_period, start_date.date(), end_date.date()))
                conn.commit()
            
            # Compute peak load statistics
            # Use a simpler approach with window functions
            if aggregation_period == "hourly":
                insert_query = f"""
                    WITH RankedData AS (
                        SELECT
                            CAST(datetime_utc AS DATE) as date_utc,
                            DATEPART(HOUR, datetime_utc) as hour_val,
                            datetime_utc,
                            global_active_power,
                            hour_of_day,
                            ROW_NUMBER() OVER (
                                PARTITION BY CAST(datetime_utc AS DATE), DATEPART(HOUR, datetime_utc)
                                ORDER BY global_active_power DESC
                            ) as rn
                        FROM power_consumption_hourly
                        WHERE datetime_utc IS NOT NULL {date_filter}
                    ),
                    PeakData AS (
                        SELECT
                            date_utc,
                            hour_val,
                            datetime_utc as peak_load_datetime,
                            global_active_power as peak_load_value,
                            hour_of_day as peak_load_hour
                        FROM RankedData
                        WHERE rn = 1
                    ),
                    AggregatedData AS (
                        SELECT
                            CAST(datetime_utc AS DATE) as date_utc,
                            DATEPART(HOUR, datetime_utc) as hour_val,
                            AVG(global_active_power) as avg_load,
                            MIN(global_active_power) as min_load,
                            VAR(global_active_power) as load_variance
                        FROM power_consumption_hourly
                        WHERE datetime_utc IS NOT NULL {date_filter}
                        GROUP BY CAST(datetime_utc AS DATE), DATEPART(HOUR, datetime_utc)
                    )
                    INSERT INTO peak_load_stats (
                        date_utc, aggregation_period,
                        peak_load_value, peak_load_datetime, peak_load_hour,
                        avg_load, min_load, load_variance,
                        peak_to_avg_ratio, load_factor
                    )
                    SELECT
                        p.date_utc,
                        ? as aggregation_period,
                        p.peak_load_value,
                        p.peak_load_datetime,
                        p.peak_load_hour,
                        a.avg_load,
                        a.min_load,
                        a.load_variance,
                        p.peak_load_value / NULLIF(a.avg_load, 0) as peak_to_avg_ratio,
                        a.avg_load / NULLIF(p.peak_load_value, 0) as load_factor
                    FROM PeakData p
                    INNER JOIN AggregatedData a ON p.date_utc = a.date_utc AND p.hour_val = a.hour_val
                    ORDER BY p.date_utc, p.hour_val
                """
            else:
                insert_query = f"""
                    WITH RankedData AS (
                        SELECT
                            CAST({date_expr} AS DATE) as date_utc,
                            datetime_utc,
                            global_active_power,
                            hour_of_day,
                            ROW_NUMBER() OVER (
                                PARTITION BY CAST({date_expr} AS DATE)
                                ORDER BY global_active_power DESC
                            ) as rn
                        FROM power_consumption_hourly
                        WHERE datetime_utc IS NOT NULL {date_filter}
                    ),
                    PeakData AS (
                        SELECT
                            date_utc,
                            datetime_utc as peak_load_datetime,
                            global_active_power as peak_load_value,
                            hour_of_day as peak_load_hour
                        FROM RankedData
                        WHERE rn = 1
                    ),
                    AggregatedData AS (
                        SELECT
                            CAST({date_expr} AS DATE) as date_utc,
                            AVG(global_active_power) as avg_load,
                            MIN(global_active_power) as min_load,
                            VAR(global_active_power) as load_variance
                        FROM power_consumption_hourly
                        WHERE datetime_utc IS NOT NULL {date_filter}
                        GROUP BY CAST({date_expr} AS DATE)
                    )
                    INSERT INTO peak_load_stats (
                        date_utc, aggregation_period,
                        peak_load_value, peak_load_datetime, peak_load_hour,
                        avg_load, min_load, load_variance,
                        peak_to_avg_ratio, load_factor
                    )
                    SELECT
                        p.date_utc,
                        ? as aggregation_period,
                        p.peak_load_value,
                        p.peak_load_datetime,
                        p.peak_load_hour,
                        a.avg_load,
                        a.min_load,
                        a.load_variance,
                        p.peak_load_value / NULLIF(a.avg_load, 0) as peak_to_avg_ratio,
                        a.avg_load / NULLIF(p.peak_load_value, 0) as load_factor
                    FROM PeakData p
                    INNER JOIN AggregatedData a ON p.date_utc = a.date_utc
                    ORDER BY p.date_utc
                """
            
            params.insert(0, aggregation_period)
            cursor.execute(insert_query, params)
            rows_affected = cursor.rowcount
            conn.commit()
            
            logger.info("Created %d peak load statistics records", rows_affected)
            return rows_affected
            
    except Exception as exc:
        logger.error("Failed to compute peak load statistics: %s", exc, exc_info=True)
        raise


def compute_weather_correlations(
    connection_string: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    aggregation_period: str = "daily"
) -> int:
    """
    Compute weather-energy correlations using Pearson correlation.
    
    Args:
        connection_string: Database connection string
        start_date: Start date for computation
        end_date: End date for computation
        aggregation_period: 'hourly', 'daily', or 'monthly'
    
    Returns:
        Number of records created
    """
    logger.info("Computing weather correlations for period: %s", aggregation_period)
    
    try:
        with pyodbc.connect(connection_string, timeout=120) as conn:
            # Load data into pandas for correlation computation
            date_filter = ""
            params = []
            if start_date:
                date_filter += " AND datetime_utc >= ?"
                params.append(start_date)
            if end_date:
                date_filter += " AND datetime_utc <= ?"
                params.append(end_date)
            
            query = f"""
                SELECT
                    datetime_utc,
                    global_active_power,
                    temperature,
                    relative_humidity,
                    pressure,
                    wind_speed,
                    precipitation
                FROM power_consumption_hourly
                WHERE datetime_utc IS NOT NULL
                  AND global_active_power IS NOT NULL {date_filter}
                ORDER BY datetime_utc
            """
            
            df = pd.read_sql(query, conn, params=params)
            
            if len(df) == 0:
                logger.warning("No data found for correlation computation")
                return 0
            
            # Aggregate by period if needed
            if aggregation_period == "daily":
                df["date"] = pd.to_datetime(df["datetime_utc"]).dt.date
                df_agg = df.groupby("date").agg({
                    "global_active_power": "mean",
                    "temperature": "mean",
                    "relative_humidity": "mean",
                    "pressure": "mean",
                    "wind_speed": "mean",
                    "precipitation": "sum"
                }).reset_index()
                date_col = "date"
            elif aggregation_period == "monthly":
                df["year_month"] = pd.to_datetime(df["datetime_utc"]).dt.to_period("M")
                df_agg = df.groupby("year_month").agg({
                    "global_active_power": "mean",
                    "temperature": "mean",
                    "relative_humidity": "mean",
                    "pressure": "mean",
                    "wind_speed": "mean",
                    "precipitation": "sum"
                }).reset_index()
                df_agg["date"] = df_agg["year_month"].dt.to_timestamp()
                date_col = "date"
            else:  # hourly
                df_agg = df.copy()
                df_agg["date"] = pd.to_datetime(df_agg["datetime_utc"]).dt.date
                date_col = "date"
            
            # Compute correlations
            correlations = []
            
            for date_val in df_agg[date_col].unique():
                date_data = df_agg[df_agg[date_col] == date_val]
                
                if len(date_data) < 2:
                    continue
                
                power = date_data["global_active_power"].values
                
                # Compute correlations
                corr_data = {
                    "date_utc": pd.to_datetime(date_val).date() if isinstance(date_val, str) else date_val,
                    "aggregation_period": aggregation_period,
                    "sample_count": len(date_data)
                }
                
                # Temperature correlation
                if "temperature" in date_data.columns and date_data["temperature"].notna().sum() > 1:
                    temp = date_data["temperature"].values
                    if len(temp) > 1 and np.std(temp) > 0:
                        corr, p_value = stats.pearsonr(power, temp)
                        corr_data["temp_power_correlation"] = corr if not np.isnan(corr) else None
                        
                        # Linear regression
                        slope, intercept, r_value, _, _ = stats.linregress(temp, power)
                        corr_data["temp_power_slope"] = slope if not np.isnan(slope) else None
                        corr_data["temp_power_intercept"] = intercept if not np.isnan(intercept) else None
                        corr_data["temp_power_r_squared"] = r_value ** 2 if not np.isnan(r_value) else None
                    else:
                        corr_data["temp_power_correlation"] = None
                else:
                    corr_data["temp_power_correlation"] = None
                
                # Humidity correlation
                if "relative_humidity" in date_data.columns and date_data["relative_humidity"].notna().sum() > 1:
                    humidity = date_data["relative_humidity"].values
                    if len(humidity) > 1 and np.std(humidity) > 0:
                        corr, _ = stats.pearsonr(power, humidity)
                        corr_data["humidity_power_correlation"] = corr if not np.isnan(corr) else None
                    else:
                        corr_data["humidity_power_correlation"] = None
                else:
                    corr_data["humidity_power_correlation"] = None
                
                # Pressure correlation
                if "pressure" in date_data.columns and date_data["pressure"].notna().sum() > 1:
                    pressure = date_data["pressure"].values
                    if len(pressure) > 1 and np.std(pressure) > 0:
                        corr, _ = stats.pearsonr(power, pressure)
                        corr_data["pressure_power_correlation"] = corr if not np.isnan(corr) else None
                    else:
                        corr_data["pressure_power_correlation"] = None
                else:
                    corr_data["pressure_power_correlation"] = None
                
                # Wind speed correlation
                if "wind_speed" in date_data.columns and date_data["wind_speed"].notna().sum() > 1:
                    wind = date_data["wind_speed"].values
                    if len(wind) > 1 and np.std(wind) > 0:
                        corr, _ = stats.pearsonr(power, wind)
                        corr_data["wind_speed_power_correlation"] = corr if not np.isnan(corr) else None
                    else:
                        corr_data["wind_speed_power_correlation"] = None
                else:
                    corr_data["wind_speed_power_correlation"] = None
                
                # Precipitation correlation
                if "precipitation" in date_data.columns and date_data["precipitation"].notna().sum() > 1:
                    precip = date_data["precipitation"].values
                    if len(precip) > 1 and np.std(precip) > 0:
                        corr, _ = stats.pearsonr(power, precip)
                        corr_data["precipitation_power_correlation"] = corr if not np.isnan(corr) else None
                    else:
                        corr_data["precipitation_power_correlation"] = None
                else:
                    corr_data["precipitation_power_correlation"] = None
                
                correlations.append(corr_data)
            
            # Delete existing correlations for the period
            cursor = conn.cursor()
            delete_query = """
                DELETE FROM weather_correlations
                WHERE aggregation_period = ? AND date_utc >= CAST(? AS DATE) AND date_utc <= CAST(? AS DATE)
            """
            if start_date and end_date:
                cursor.execute(delete_query, (aggregation_period, start_date.date(), end_date.date()))
                conn.commit()
            
            # Insert correlations
            insert_query = """
                INSERT INTO weather_correlations (
                    date_utc, aggregation_period,
                    temp_power_correlation, humidity_power_correlation,
                    pressure_power_correlation, wind_speed_power_correlation,
                    precipitation_power_correlation,
                    temp_power_slope, temp_power_intercept, temp_power_r_squared,
                    sample_count
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            rows_inserted = 0
            for corr_data in correlations:
                try:
                    cursor.execute(insert_query, (
                        corr_data["date_utc"],
                        corr_data["aggregation_period"],
                        corr_data.get("temp_power_correlation"),
                        corr_data.get("humidity_power_correlation"),
                        corr_data.get("pressure_power_correlation"),
                        corr_data.get("wind_speed_power_correlation"),
                        corr_data.get("precipitation_power_correlation"),
                        corr_data.get("temp_power_slope"),
                        corr_data.get("temp_power_intercept"),
                        corr_data.get("temp_power_r_squared"),
                        corr_data["sample_count"]
                    ))
                    rows_inserted += 1
                except Exception as exc:
                    logger.warning("Failed to insert correlation for %s: %s", corr_data["date_utc"], exc)
            
            conn.commit()
            logger.info("Created %d weather correlation records", rows_inserted)
            return rows_inserted
            
    except Exception as exc:
        logger.error("Failed to compute weather correlations: %s", exc, exc_info=True)
        raise


def run_all_feature_engineering(
    connection_string: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> dict:
    """
    Run all feature engineering tasks.
    
    Returns:
        Dictionary with counts of records created for each feature table
    """
    logger.info("Running all feature engineering tasks")
    
    results = {}
    
    # Daily aggregates
    results["daily_aggregates"] = compute_daily_aggregates(
        connection_string, start_date, end_date
    )
    
    # Peak load statistics (daily and monthly)
    results["peak_load_daily"] = compute_peak_load_stats(
        connection_string, start_date, end_date, "daily"
    )
    results["peak_load_monthly"] = compute_peak_load_stats(
        connection_string, start_date, end_date, "monthly"
    )
    
    # Weather correlations (daily)
    results["weather_correlations_daily"] = compute_weather_correlations(
        connection_string, start_date, end_date, "daily"
    )
    
    logger.info("Feature engineering completed: %s", results)
    return results

