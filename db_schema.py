"""
Database Schema Setup for Time-Series Data
Creates optimized tables with partitioning for time-series power consumption data.
"""

import logging
from datetime import datetime
from typing import Optional

import pyodbc

logger = logging.getLogger(__name__)


# def create_time_series_schema(connection_string: str) -> None:
#     """
#     Create database schema optimized for time-series data with partitioning.

#     Tables created:
#     - power_consumption_hourly: Main time-series table with partitioning by month
#     - power_consumption_daily: Daily aggregated features
#     - peak_load_stats: Peak load statistics by day/month
#     - weather_correlations: Weather-energy correlation metrics
#     """
#     logger.info("Creating time-series database schema...")

#     try:
#         with pyodbc.connect(connection_string, timeout=30) as conn:
#             cursor = conn.cursor()

#             # Create main time-series table with partitioning
#             create_main_table(cursor)

#             # Create aggregated feature tables
#             create_daily_aggregates_table(cursor)
#             create_peak_load_stats_table(cursor)
#             create_weather_correlations_table(cursor)

#             # Create indexes for query performance
#             create_indexes(cursor)

#             conn.commit()
#             logger.info("Database schema created successfully")

#     except Exception as exc:
#         logger.error("Failed to create database schema: %s", exc, exc_info=True)
#         raise


def create_time_series_schema(connection_string: str):
    try:
        with pyodbc.connect(connection_string, timeout=60) as conn:
            cursor = conn.cursor()

            # Check if table exists
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM sys.tables
                WHERE name = 'power_consumption_hourly';
            """
            )
            exists = cursor.fetchone()[0]

            if exists:
                logger.info("Main table already exists — skipping schema creation.")
                return

            # If not exists, create everything
            logger.info("Creating time-series schema...")
            create_main_table(cursor)

            conn.commit()
            logger.info("Schema created successfully.")
    except Exception as e:
        logger.error("Schema creation failed: %s", e)
        raise


def create_main_table(cursor: pyodbc.Cursor) -> None:
    """Create the main time-series table with partitioning support."""

    # Drop table if exists
    cursor.execute(
        """
        IF OBJECT_ID('power_consumption_hourly', 'U') IS NOT NULL
        DROP TABLE power_consumption_hourly;
    """
    )

    # Create main table with partitioning by month
    # SQL Server partitioning requires a partition function and scheme
    # For simplicity, we'll use a computed column for partitioning key
    cursor.execute(
        """
        CREATE TABLE power_consumption_hourly (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            datetime_utc DATETIME2 NOT NULL,
            year_month AS (CAST(YEAR(datetime_utc) AS VARCHAR(4)) + '-' + 
                          RIGHT('0' + CAST(MONTH(datetime_utc) AS VARCHAR(2)), 2)) PERSISTED,
            
            -- Power consumption metrics
            global_active_power FLOAT NOT NULL,
            global_reactive_power FLOAT,
            voltage FLOAT,
            global_intensity FLOAT,
            sub_metering_1 FLOAT,
            sub_metering_2 FLOAT,
            sub_metering_3 FLOAT,
            
            -- Weather metrics
            temperature FLOAT,
            dewpoint FLOAT,
            relative_humidity FLOAT,
            precipitation FLOAT,
            wind_direction FLOAT,
            wind_speed FLOAT,
            pressure FLOAT,
            
            -- Engineered features
            hour_of_day TINYINT,
            day_of_week TINYINT,
            month_of_year TINYINT,
            is_weekend BIT,
            lag_power_1h FLOAT,
            lag_power_24h FLOAT,
            lag_temp_1h FLOAT,
            
            -- Metadata
            created_at DATETIME2 DEFAULT GETUTCDATE(),
            updated_at DATETIME2 DEFAULT GETUTCDATE(),
            
            -- Constraints
            CONSTRAINT CK_datetime_valid CHECK (datetime_utc >= '2000-01-01'),
            CONSTRAINT CK_power_positive CHECK (global_active_power >= 0)
        );
    """
    )

    # Create partition function (monthly partitioning)
    try:
        cursor.execute(
            """
            IF EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'PF_PowerConsumption_Monthly')
            DROP PARTITION FUNCTION PF_PowerConsumption_Monthly;
        """
        )
        cursor.execute(
            """
            CREATE PARTITION FUNCTION PF_PowerConsumption_Monthly (VARCHAR(7))
            AS RANGE RIGHT FOR VALUES 
            ('2006-12', '2007-01', '2007-02', '2007-03', '2007-04', '2007-05', '2007-06',
             '2007-07', '2007-08', '2007-09', '2007-10', '2007-11', '2007-12',
             '2008-01', '2008-02', '2008-03', '2008-04', '2008-05', '2008-06',
             '2008-07', '2008-08', '2008-09', '2008-10', '2008-11', '2008-12',
             '2009-01', '2009-02', '2009-03', '2009-04', '2009-05', '2009-06',
             '2009-07', '2009-08', '2009-09', '2009-10', '2009-11', '2009-12',
             '2010-01', '2010-02', '2010-03', '2010-04', '2010-05', '2010-06',
             '2010-07', '2010-08', '2010-09', '2010-10', '2010-11', '2010-12');
        """
        )

        # Create partition scheme
        cursor.execute(
            """
            IF EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'PS_PowerConsumption_Monthly')
            DROP PARTITION SCHEME PS_PowerConsumption_Monthly;
        """
        )
        cursor.execute(
            """
            CREATE PARTITION SCHEME PS_PowerConsumption_Monthly
            AS PARTITION PF_PowerConsumption_Monthly
            ALL TO ([PRIMARY]);
        """
        )

        # Recreate table with partition scheme
        cursor.execute("DROP TABLE power_consumption_hourly;")
        cursor.execute(
            """
            CREATE TABLE power_consumption_hourly (
                id BIGINT IDENTITY(1,1),
                datetime_utc DATETIME2 NOT NULL,
                year_month AS (CAST(YEAR(datetime_utc) AS VARCHAR(4)) + '-' + 
                              RIGHT('0' + CAST(MONTH(datetime_utc) AS VARCHAR(2)), 2)) PERSISTED,
                
                global_active_power FLOAT NOT NULL,
                global_reactive_power FLOAT,
                voltage FLOAT,
                global_intensity FLOAT,
                sub_metering_1 FLOAT,
                sub_metering_2 FLOAT,
                sub_metering_3 FLOAT,
                
                temperature FLOAT,
                dewpoint FLOAT,
                relative_humidity FLOAT,
                precipitation FLOAT,
                wind_direction FLOAT,
                wind_speed FLOAT,
                pressure FLOAT,
                
                hour_of_day TINYINT,
                day_of_week TINYINT,
                month_of_year TINYINT,
                is_weekend BIT,
                lag_power_1h FLOAT,
                lag_power_24h FLOAT,
                lag_temp_1h FLOAT,
                
                created_at DATETIME2 DEFAULT GETUTCDATE(),
                updated_at DATETIME2 DEFAULT GETUTCDATE(),
                
                CONSTRAINT PK_power_consumption_hourly PRIMARY KEY (id, year_month),
                CONSTRAINT CK_datetime_valid CHECK (datetime_utc >= '2000-01-01'),
                CONSTRAINT CK_power_positive CHECK (global_active_power >= 0)
            ) ON PS_PowerConsumption_Monthly(year_month);
        """
        )
        logger.info("Created partitioned table: power_consumption_hourly")
    except Exception as exc:
        logger.warning(
            "Partitioning setup failed, using non-partitioned table: %s", exc
        )
        # Fallback to non-partitioned table
        cursor.execute(
            """
            CREATE TABLE power_consumption_hourly (
                id BIGINT IDENTITY(1,1) PRIMARY KEY,
                datetime_utc DATETIME2 NOT NULL,
                year_month AS (CAST(YEAR(datetime_utc) AS VARCHAR(4)) + '-' + 
                              RIGHT('0' + CAST(MONTH(datetime_utc) AS VARCHAR(2)), 2)) PERSISTED,
                
                global_active_power FLOAT NOT NULL,
                global_reactive_power FLOAT,
                voltage FLOAT,
                global_intensity FLOAT,
                sub_metering_1 FLOAT,
                sub_metering_2 FLOAT,
                sub_metering_3 FLOAT,
                
                temperature FLOAT,
                dewpoint FLOAT,
                relative_humidity FLOAT,
                precipitation FLOAT,
                wind_direction FLOAT,
                wind_speed FLOAT,
                pressure FLOAT,
                
                hour_of_day TINYINT,
                day_of_week TINYINT,
                month_of_year TINYINT,
                is_weekend BIT,
                lag_power_1h FLOAT,
                lag_power_24h FLOAT,
                lag_temp_1h FLOAT,
                
                created_at DATETIME2 DEFAULT GETUTCDATE(),
                updated_at DATETIME2 DEFAULT GETUTCDATE(),
                
                CONSTRAINT CK_datetime_valid CHECK (datetime_utc >= '2000-01-01'),
                CONSTRAINT CK_power_positive CHECK (global_active_power >= 0)
            );
        """
        )


def create_daily_aggregates_table(cursor: pyodbc.Cursor) -> None:
    """Create table for daily aggregated features."""
    cursor.execute(
        """
        IF OBJECT_ID('power_consumption_daily', 'U') IS NOT NULL
        DROP TABLE power_consumption_daily;
    """
    )

    cursor.execute(
        """
        CREATE TABLE power_consumption_daily (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            date_utc DATE NOT NULL UNIQUE,
            
            -- Daily aggregates
            total_active_power FLOAT NOT NULL,
            avg_active_power FLOAT NOT NULL,
            max_active_power FLOAT NOT NULL,
            min_active_power FLOAT NOT NULL,
            std_active_power FLOAT,
            
            total_reactive_power FLOAT,
            avg_reactive_power FLOAT,
            
            avg_voltage FLOAT,
            avg_intensity FLOAT,
            
            total_sub_metering_1 FLOAT,
            total_sub_metering_2 FLOAT,
            total_sub_metering_3 FLOAT,
            
            -- Weather aggregates
            avg_temperature FLOAT,
            max_temperature FLOAT,
            min_temperature FLOAT,
            avg_humidity FLOAT,
            total_precipitation FLOAT,
            avg_wind_speed FLOAT,
            avg_pressure FLOAT,
            
            -- Day characteristics
            day_of_week TINYINT,
            month_of_year TINYINT,
            is_weekend BIT,
            
            -- Metadata
            created_at DATETIME2 DEFAULT GETUTCDATE(),
            updated_at DATETIME2 DEFAULT GETUTCDATE()
        );
    """
    )
    logger.info("Created table: power_consumption_daily")


def create_peak_load_stats_table(cursor: pyodbc.Cursor) -> None:
    """Create table for peak load statistics."""
    cursor.execute(
        """
        IF OBJECT_ID('peak_load_stats', 'U') IS NOT NULL
        DROP TABLE peak_load_stats;
    """
    )

    cursor.execute(
        """
        CREATE TABLE peak_load_stats (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            date_utc DATE NOT NULL,
            aggregation_period VARCHAR(20) NOT NULL, -- 'hourly', 'daily', 'monthly'
            
            peak_load_value FLOAT NOT NULL,
            peak_load_datetime DATETIME2 NOT NULL,
            peak_load_hour TINYINT,
            
            avg_load FLOAT NOT NULL,
            min_load FLOAT NOT NULL,
            load_variance FLOAT,
            
            peak_to_avg_ratio FLOAT,
            load_factor FLOAT, -- avg_load / peak_load
            
            -- Metadata
            created_at DATETIME2 DEFAULT GETUTCDATE(),
            
            CONSTRAINT UQ_peak_load_date_period UNIQUE (date_utc, aggregation_period)
        );
    """
    )
    logger.info("Created table: peak_load_stats")


def create_weather_correlations_table(cursor: pyodbc.Cursor) -> None:
    """Create table for weather-energy correlation metrics."""
    cursor.execute(
        """
        IF OBJECT_ID('weather_correlations', 'U') IS NOT NULL
        DROP TABLE weather_correlations;
    """
    )

    cursor.execute(
        """
        CREATE TABLE weather_correlations (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            date_utc DATE NOT NULL,
            aggregation_period VARCHAR(20) NOT NULL, -- 'hourly', 'daily', 'monthly'
            
            -- Correlation coefficients
            temp_power_correlation FLOAT,
            humidity_power_correlation FLOAT,
            pressure_power_correlation FLOAT,
            wind_speed_power_correlation FLOAT,
            precipitation_power_correlation FLOAT,
            
            -- Regression metrics
            temp_power_slope FLOAT,
            temp_power_intercept FLOAT,
            temp_power_r_squared FLOAT,
            
            -- Sample size
            sample_count INT NOT NULL,
            
            -- Metadata
            created_at DATETIME2 DEFAULT GETUTCDATE(),
            
            CONSTRAINT UQ_weather_corr_date_period UNIQUE (date_utc, aggregation_period)
        );
    """
    )
    logger.info("Created table: weather_correlations")


def create_indexes(cursor: pyodbc.Cursor) -> None:
    """Create indexes for query performance."""

    # Main table indexes
    cursor.execute(
        """
        CREATE NONCLUSTERED INDEX IX_power_consumption_datetime 
        ON power_consumption_hourly(datetime_utc);
    """
    )

    cursor.execute(
        """
        CREATE NONCLUSTERED INDEX IX_power_consumption_year_month 
        ON power_consumption_hourly(year_month);
    """
    )

    cursor.execute(
        """
        CREATE NONCLUSTERED INDEX IX_power_consumption_hour_day 
        ON power_consumption_hourly(hour_of_day, day_of_week);
    """
    )

    # Daily aggregates indexes
    cursor.execute(
        """
        CREATE NONCLUSTERED INDEX IX_daily_date 
        ON power_consumption_daily(date_utc);
    """
    )

    cursor.execute(
        """
        CREATE NONCLUSTERED INDEX IX_daily_month_weekend 
        ON power_consumption_daily(month_of_year, is_weekend);
    """
    )

    logger.info("Created indexes for query optimization")


def get_schema_info(connection_string: str) -> dict:
    """Get information about the database schema."""
    info = {"tables": [], "indexes": [], "partitions": []}

    try:
        with pyodbc.connect(connection_string, timeout=30) as conn:
            cursor = conn.cursor()

            # Get table information
            cursor.execute(
                """
                SELECT TABLE_NAME, 
                       (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = t.TABLE_NAME) as COLUMN_COUNT
                FROM INFORMATION_SCHEMA.TABLES t
                WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_NAME LIKE 'power_%' OR TABLE_NAME LIKE 'peak_%' OR TABLE_NAME LIKE 'weather_%'
                ORDER BY TABLE_NAME;
            """
            )

            for row in cursor.fetchall():
                info["tables"].append({"name": row[0], "column_count": row[1]})

            # Get partition information
            cursor.execute(
                """
                SELECT p.partition_number, p.rows, pf.name as partition_function
                FROM sys.partitions p
                INNER JOIN sys.partition_functions pf ON p.function_id = pf.function_id
                WHERE OBJECT_NAME(p.object_id) = 'power_consumption_hourly'
                ORDER BY p.partition_number;
            """
            )

            for row in cursor.fetchall():
                info["partitions"].append(
                    {"partition_number": row[0], "rows": row[1], "function": row[2]}
                )

    except Exception as exc:
        logger.warning("Could not retrieve schema info: %s", exc)

    return info
