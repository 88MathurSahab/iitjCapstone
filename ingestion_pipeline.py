"""
Data Ingestion Pipeline for Time-Series Power Consumption Data
Loads processed data from CSV or Azure Blob Storage into the database.
"""

import io
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient, ContainerClient

logger = logging.getLogger(__name__)


def load_data_from_csv(file_path: Path) -> pd.DataFrame:
    """Load processed data from CSV file."""
    logger.info("Loading data from CSV: %s", file_path)
    df = pd.read_csv(file_path, parse_dates=["DateTime"], index_col="DateTime")
    logger.info("Loaded %d rows from CSV", len(df))
    return df


def load_data_from_azure(
    connection_string: str, container_name: str, blob_name: str
) -> pd.DataFrame:
    """Load processed data from Azure Blob Storage."""
    logger.info("Loading data from Azure Blob: %s/%s", container_name, blob_name)

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob=blob_name)

    # Download blob content
    blob_data = blob_client.download_blob()
    content = blob_data.readall()

    # Parse CSV from bytes
    df = pd.read_csv(
        io.BytesIO(content), parse_dates=["DateTime"], index_col="DateTime"
    )
    logger.info("Loaded %d rows from Azure Blob", len(df))
    return df


def prepare_data_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare DataFrame for database insertion."""
    logger.info("Preparing data for database insertion")

    # Reset index to make DateTime a column
    df_db = df.reset_index()

    # Rename columns to match database schema
    column_mapping = {
        "DateTime": "datetime_utc",
        "Global_active_power": "global_active_power",
        "Global_reactive_power": "global_reactive_power",
        "Voltage": "voltage",
        "Global_intensity": "global_intensity",
        "Sub_metering_1": "sub_metering_1",
        "Sub_metering_2": "sub_metering_2",
        "Sub_metering_3": "sub_metering_3",
        "temp": "temperature",
        "dwpt": "dewpoint",
        "rhum": "relative_humidity",
        "prcp": "precipitation",
        "wdir": "wind_direction",
        "wspd": "wind_speed",
        "pres": "pressure",
        "hour_of_day": "hour_of_day",
        "day_of_week": "day_of_week",
        "month_of_year": "month_of_year",
        "is_weekend": "is_weekend",
        "lag_power_1h": "lag_power_1h",
        "lag_power_24h": "lag_power_24h",
        "lag_temp_1h": "lag_temp_1h",
    }

    # Select and rename columns
    df_db = df_db.rename(columns=column_mapping)

    # Convert is_weekend to boolean (0/1 to False/True)
    if "is_weekend" in df_db.columns:
        df_db["is_weekend"] = df_db["is_weekend"].astype(bool)

    # Ensure datetime is timezone-aware (UTC)
    if df_db["datetime_utc"].dt.tz is None:
        df_db["datetime_utc"] = pd.to_datetime(df_db["datetime_utc"]).dt.tz_localize(
            "UTC"
        )
    else:
        df_db["datetime_utc"] = df_db["datetime_utc"].dt.tz_convert("UTC")

    # Select only columns that exist in the mapping
    available_columns = [col for col in column_mapping.values() if col in df_db.columns]
    df_db = df_db[available_columns]

    logger.info("Prepared %d rows with %d columns", len(df_db), len(df_db.columns))
    return df_db


def ingest_data_batch(
    connection_string: str,
    df: pd.DataFrame,
    batch_size: int = 1000,
    table_name: str = "power_consumption_hourly",
) -> int:
    """
    Ingest data into database in batches for better performance.

    Returns:
        Number of rows inserted
    """
    logger.info(
        "Starting batch ingestion into %s (batch size: %d)", table_name, batch_size
    )

    df_db = prepare_data_for_db(df)
    total_rows = len(df_db)
    rows_inserted = 0

    try:
        with pyodbc.connect(connection_string, timeout=60) as conn:
            cursor = conn.cursor()

            # Get column names
            columns = list(df_db.columns)
            placeholders = ", ".join(["?"] * len(columns))
            column_names = ", ".join(columns)

            insert_query = f"""
                INSERT INTO {table_name} ({column_names})
                VALUES ({placeholders})
            """

            # Process in batches
            for i in range(0, total_rows, batch_size):
                batch = df_db.iloc[i : i + batch_size]

                # Convert DataFrame rows to list of tuples
                values = []
                for _, row in batch.iterrows():
                    # Convert to tuple, handling NaN values
                    row_values = []
                    for val in row:
                        if pd.isna(val):
                            row_values.append(None)
                        else:
                            row_values.append(val)
                    values.append(tuple(row_values))

                # Execute batch insert
                try:
                    cursor.executemany(insert_query, values)
                    conn.commit()
                    rows_inserted += len(batch)
                    logger.debug(
                        "Inserted batch %d-%d (%d rows)", i, i + len(batch), len(batch)
                    )
                except Exception as exc:
                    logger.error(
                        "Error inserting batch %d-%d: %s", i, i + len(batch), exc
                    )
                    conn.rollback()
                    # Try inserting row by row to identify problematic rows
                    for idx, row_tuple in enumerate(values):
                        try:
                            cursor.execute(insert_query, row_tuple)
                            conn.commit()
                            rows_inserted += 1
                        except Exception as row_exc:
                            logger.warning("Skipped row %d: %s", i + idx, row_exc)
                            conn.rollback()

            logger.info("Successfully inserted %d/%d rows", rows_inserted, total_rows)

    except Exception as exc:
        logger.error("Failed to ingest data: %s", exc, exc_info=True)
        raise

    return rows_inserted


def ingest_data_streaming(
    connection_string: str,
    data_source: str,
    source_type: str = "csv",
    azure_connection_string: Optional[str] = None,
    container_name: Optional[str] = None,
    batch_size: int = 1000,
) -> int:
    """
    Stream data from source and ingest into database.

    Args:
        connection_string: Database connection string
        data_source: Path to CSV file or blob name in Azure
        source_type: 'csv' or 'azure'
        azure_connection_string: Azure storage connection string (if source_type is 'azure')
        container_name: Azure container name (if source_type is 'azure')
        batch_size: Number of rows to process per batch

    Returns:
        Number of rows inserted
    """
    logger.info("Starting streaming ingestion from %s: %s", source_type, data_source)

    if source_type == "csv":
        df = load_data_from_csv(Path(data_source))
    elif source_type == "azure":
        if not azure_connection_string or not container_name:
            raise ValueError(
                "azure_connection_string and container_name required for Azure source"
            )
        df = load_data_from_azure(azure_connection_string, container_name, data_source)
    else:
        raise ValueError(f"Unknown source_type: {source_type}")

    return ingest_data_batch(connection_string, df, batch_size)


def check_existing_data(
    connection_string: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> dict:
    """Check what data already exists in the database."""
    logger.info("Checking existing data in database")

    try:
        with pyodbc.connect(connection_string, timeout=30) as conn:
            cursor = conn.cursor()

            # Get date range of existing data
            query = """
                SELECT 
                    MIN(datetime_utc) as min_date,
                    MAX(datetime_utc) as max_date,
                    COUNT(*) as row_count
                FROM power_consumption_hourly
            """

            if start_date or end_date:
                conditions = []
                if start_date:
                    conditions.append("datetime_utc >= ?")
                if end_date:
                    conditions.append("datetime_utc <= ?")
                query += " WHERE " + " AND ".join(conditions)

            params = []
            if start_date:
                params.append(start_date)
            if end_date:
                params.append(end_date)

            cursor.execute(query, params)
            result = cursor.fetchone()

            if result and result[2] > 0:
                return {
                    "exists": True,
                    "min_date": result[0],
                    "max_date": result[1],
                    "row_count": result[2],
                }
            else:
                return {"exists": False, "row_count": 0}

    except Exception as exc:
        logger.warning("Could not check existing data: %s", exc)
        return {"exists": False, "error": str(exc)}


def reset_main_table(connection_string: str):
    logger.info("Clearing existing data from main table...")

    try:
        with pyodbc.connect(connection_string, timeout=60) as conn:
            cursor = conn.cursor()

            # Delete all rows
            cursor.execute("DELETE FROM power_consumption_hourly;")
            conn.commit()

            logger.info("Old data cleared successfully.")

    except Exception as e:
        logger.error("Error clearing old data: %s", e)
        raise


def upsert_data(
    connection_string: str, df: pd.DataFrame, batch_size: int = 1000
) -> int:
    logger.info("Starting upsert operation")

    df_db = prepare_data_for_db(df)
    total_rows = len(df_db)
    rows_processed = 0

    columns = list(df_db.columns)
    column_names = ", ".join(columns)
    source_columns = ", ".join([f"source.{c}" for c in columns])
    placeholders = ", ".join(["?"] * len(columns))

    # UPDATE clause (exclude datetime_utc)
    update_columns = [c for c in columns if c != "datetime_utc"]
    update_clause = ", ".join([f"{c} = source.{c}" for c in update_columns])

    merge_query = f"""
        MERGE power_consumption_hourly AS target
        USING (VALUES ({placeholders})) AS source ({column_names})
        ON target.datetime_utc = source.datetime_utc
        WHEN MATCHED THEN UPDATE SET
            {update_clause},
            updated_at = GETUTCDATE()
        WHEN NOT MATCHED THEN INSERT ({column_names})
        VALUES ({source_columns});
    """

    try:
        with pyodbc.connect(connection_string, timeout=60) as conn:
            cursor = conn.cursor()

            for i in range(0, total_rows, batch_size):
                batch = df_db.iloc[i : i + batch_size]
                values = [
                    tuple(None if pd.isna(v) else v for v in row)
                    for _, row in batch.iterrows()
                ]

                try:
                    cursor.executemany(merge_query, values)
                    conn.commit()
                    rows_processed += len(batch)
                except Exception as exc:
                    logger.error(
                        "Error upserting batch %d-%d: %s", i, i + len(batch), exc
                    )
                    conn.rollback()

            logger.info("Successfully processed %d/%d rows", rows_processed, total_rows)

    except Exception as exc:
        logger.error("Failed to upsert data: %s", exc, exc_info=True)
        raise

    return rows_processed

    # def upsert_data(
    #     connection_string: str, df: pd.DataFrame, batch_size: int = 1000
    # ) -> int:
    """
    Upsert data (insert or update if exists based on datetime_utc).

    Returns:
        Number of rows processed
    """
    logger.info("Starting upsert operation")

    df_db = prepare_data_for_db(df)
    total_rows = len(df_db)
    rows_processed = 0

    try:
        with pyodbc.connect(connection_string, timeout=60) as conn:
            cursor = conn.cursor()

            columns = list(df_db.columns)
            placeholders = ", ".join(["?"] * len(columns))
            column_names = ", ".join(columns)

            # Build UPDATE clause (exclude datetime_utc from update)
            update_columns = [col for col in columns if col != "datetime_utc"]
            update_clause = ", ".join([f"{col} = ?" for col in update_columns])

            upsert_query = f"""
                MERGE power_consumption_hourly AS target
                USING (VALUES ({placeholders})) AS source ({column_names})
                ON target.datetime_utc = source.datetime_utc
                WHEN MATCHED THEN
                    UPDATE SET {update_clause}, updated_at = GETUTCDATE()
                WHEN NOT MATCHED THEN
                    INSERT ({column_names})
                    VALUES ({placeholders});
            """

            # Process in batches
            for i in range(0, total_rows, batch_size):
                batch = df_db.iloc[i : i + batch_size]

                values = []
                for _, row in batch.iterrows():
                    row_values = []
                    for val in row:
                        if pd.isna(val):
                            row_values.append(None)
                        else:
                            row_values.append(val)
                    # For MERGE, we need values twice (for source and INSERT)
                    values.append(tuple(row_values))

                try:
                    cursor.executemany(upsert_query, values)
                    conn.commit()
                    rows_processed += len(batch)
                    logger.debug("Upserted batch %d-%d", i, i + len(batch))
                except Exception as exc:
                    logger.error(
                        "Error upserting batch %d-%d: %s", i, i + len(batch), exc
                    )
                    conn.rollback()

            logger.info("Successfully processed %d/%d rows", rows_processed, total_rows)

    except Exception as exc:
        logger.error("Failed to upsert data: %s", exc, exc_info=True)
        raise

    return rows_processed
