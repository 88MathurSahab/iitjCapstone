from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pyodbc
import requests
from azure.storage.blob import BlobServiceClient, ContainerClient
from meteostat import Hourly

logger = logging.getLogger(__name__)

DEFAULT_SQL_CONNECTION_STRING = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=tcp:greenpowerutilities-dbserver.database.windows.net,1433;"
    "DATABASE=GreenPowerUtilities_DB;"
    "Uid=CloudSAe9b21290;"
    "Pwd=Yash@8896;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

DEFAULT_AZURE_CONNECTION = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=greenpowerstorage;"
    "AccountKey=GNvSIF/x7DFrDBBDiuyRVBRjGBs3J41+TNnUBjiiO0Pj/BImCvK7Bp5fCnzmYw+RV5ucKtrT17D4+AStt6eZ5Q==;"
    "EndpointSuffix=core.windows.net"
)

FILE_TO_EXTRACT = "household_power_consumption.txt"


@dataclass
class PipelineConfig:
    zip_url: str
    raw_filename: str
    azure_connection_string: str
    container_name: str
    raw_blob_name: str
    merged_blob_name: str
    output_path: Path
    station_id: str
    start: datetime
    end: datetime
    sql_connection_string: Optional[str] = None


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


def test_database_connection(connection_string: str) -> None:
    logger.info("Testing database connectivity...")
    try:
        with pyodbc.connect(connection_string, timeout=10) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT GETDATE()")
            timestamp = cursor.fetchone()[0]
        logger.info("Database connection successful (server time: %s)", timestamp)
    except Exception as exc:
        logger.warning("Database connection failed: %s", exc)


def download_zip(url: str, timeout: int = 60) -> bytes:
    logger.info("Downloading dataset from %s", url)
    response = requests.get(url, stream=True, timeout=timeout)
    response.raise_for_status()
    content = response.content
    logger.debug("Downloaded %d bytes", len(content))
    return content


def extract_from_zip(zip_bytes: bytes, file_name: str) -> bytes:
    logger.info("Extracting %s from archive", file_name)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        if file_name not in archive.namelist():
            raise FileNotFoundError(f"{file_name} not found in archive")
        data = archive.read(file_name)
    logger.debug("Extracted %d bytes from %s", len(data), file_name)
    return data


def ensure_container(container_client: ContainerClient) -> None:
    try:
        container_client.create_container()
        logger.info("Created Azure container %s", container_client.container_name)
    except Exception as exc:
        if "ContainerAlreadyExists" in str(exc):
            logger.debug("Container %s already exists", container_client.container_name)
        else:
            raise


def upload_blob_bytes(
    container_client: ContainerClient, blob_path: str, data: bytes
) -> None:
    blob_client = container_client.get_blob_client(blob=blob_path)
    logger.info("Uploading %s (%d bytes) to Azure Blob Storage", blob_path, len(data))
    blob_client.upload_blob(data, overwrite=True)


def upload_blob_file(
    container_client: ContainerClient, blob_path: str, file_path: Path
) -> None:
    blob_client = container_client.get_blob_client(blob=blob_path)
    logger.info("Uploading %s to Azure Blob Storage", blob_path)
    with file_path.open("rb") as stream:
        blob_client.upload_blob(stream, overwrite=True)


def load_power_dataframe(raw_bytes: bytes) -> pd.DataFrame:
    logger.info("Parsing power consumption dataset")
    data_stream = io.StringIO(raw_bytes.decode("utf-8"))
    df = pd.read_csv(
        data_stream,
        sep=";",
        na_values=["?"],
        parse_dates={"DateTime": ["Date", "Time"]},
        dayfirst=True,
        index_col="DateTime",
    )
    numeric_columns = [
        "Global_active_power",
        "Global_reactive_power",
        "Voltage",
        "Global_intensity",
        "Sub_metering_1",
        "Sub_metering_2",
        "Sub_metering_3",
    ]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)
    logger.debug("Power dataframe shape: %s", df.shape)
    return df


def resample_to_hourly(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Resampling power data to hourly frequency")
    aggregation_rules = {
        "Global_active_power": "mean",
        "Global_reactive_power": "mean",
        "Voltage": "mean",
        "Global_intensity": "mean",
        "Sub_metering_1": "sum",
        "Sub_metering_2": "sum",
        "Sub_metering_3": "sum",
    }
    hourly = df.resample("H").agg(aggregation_rules)
    logger.debug("Hourly dataframe shape: %s", hourly.shape)
    return hourly


def fetch_weather_data(station_id: str, start: datetime, end: datetime) -> pd.DataFrame:
    logger.info(
        "Fetching weather data for station %s between %s and %s",
        station_id,
        start,
        end,
    )
    data = Hourly(station_id, start, end)
    df_weather = data.fetch()
    logger.debug("Weather dataframe shape: %s", df_weather.shape)
    return df_weather


def merge_datasets(power_df: pd.DataFrame, weather_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Merging power and weather datasets on DateTime index")
    merged = pd.merge(
        power_df,
        weather_df,
        left_index=True,
        right_index=True,
        how="inner",
    )
    logger.debug("Merged dataframe shape: %s", merged.shape)
    return merged


def save_dataframe(df: pd.DataFrame, output_path: Path) -> None:
    logger.info("Writing merged dataset to %s", output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(
        output_path, encoding="utf-8", sep=",", index=True, index_label="DateTime"
    )


def build_config(args: argparse.Namespace) -> PipelineConfig:
    azure_connection = os.getenv("AZURE_CONNECTION_STRING", DEFAULT_AZURE_CONNECTION)
    container_name = os.getenv("AZURE_CONTAINER_NAME", "greenpowerstorage-container")
    raw_prefix = os.getenv("AZURE_RAW_PREFIX", "raw")
    processed_prefix = os.getenv("AZURE_PROCESSED_PREFIX", "processed")

    raw_blob_name = f"{raw_prefix}/{FILE_TO_EXTRACT}" if raw_prefix else FILE_TO_EXTRACT
    merged_blob_name = (
        f"{processed_prefix}/merged_power_and_weather.csv"
        if processed_prefix
        else "merged_power_and_weather.csv"
    )

    output_path = Path(
        args.output or os.getenv("OUTPUT_CSV_PATH", "merged_power_and_weather.csv")
    )

    sql_connection_string = os.getenv(
        "SQL_CONNECTION_STRING", DEFAULT_SQL_CONNECTION_STRING
    )

    return PipelineConfig(
        zip_url=args.zip_url,
        raw_filename=FILE_TO_EXTRACT,
        azure_connection_string=azure_connection,
        container_name=container_name,
        raw_blob_name=raw_blob_name,
        merged_blob_name=merged_blob_name,
        output_path=output_path,
        station_id=args.station_id,
        start=args.start,
        end=args.end,
        sql_connection_string=sql_connection_string if args.test_db else None,
    )


def run_pipeline(
    config: PipelineConfig,
    upload_raw: bool,
    upload_merged: bool,
) -> Path:
    zip_bytes = download_zip(config.zip_url)
    raw_bytes = extract_from_zip(zip_bytes, config.raw_filename)

    blob_container: Optional[ContainerClient] = None
    if upload_raw or upload_merged:
        blob_service_client = BlobServiceClient.from_connection_string(
            config.azure_connection_string
        )
        blob_container = blob_service_client.get_container_client(config.container_name)
        ensure_container(blob_container)

    if upload_raw and blob_container is not None:
        upload_blob_bytes(blob_container, config.raw_blob_name, raw_bytes)

    power_df = load_power_dataframe(raw_bytes)
    hourly_power = resample_to_hourly(power_df)
    weather_df = fetch_weather_data(config.station_id, config.start, config.end)
    merged_df = merge_datasets(hourly_power, weather_df)

    save_dataframe(merged_df, config.output_path)

    if upload_merged and blob_container is not None:
        upload_blob_file(blob_container, config.merged_blob_name, config.output_path)

    return config.output_path


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build merged power and weather dataset and publish to Azure Blob Storage."
    )
    parser.add_argument(
        "--zip-url",
        default="https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip",
        help="Source ZIP file URL for the power consumption dataset.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Local path for the merged CSV output (defaults to OUTPUT_CSV_PATH env or ./merged_power_and_weather.csv).",
    )
    parser.add_argument(
        "--station-id",
        default=os.getenv("METEOSTAT_STATION_ID", "07149"),
        help="Meteostat station ID to fetch weather data for.",
    )
    parser.add_argument(
        "--start",
        type=lambda value: datetime.fromisoformat(value),
        default=datetime(2006, 12, 16),
        help="Start datetime (ISO format) for weather data.",
    )
    parser.add_argument(
        "--end",
        type=lambda value: datetime.fromisoformat(value),
        default=datetime(2010, 11, 26, 23, 59),
        help="End datetime (ISO format) for weather data.",
    )
    parser.add_argument(
        "--skip-raw-upload",
        action="store_true",
        help="Skip uploading the raw text dataset to Azure Blob Storage.",
    )
    parser.add_argument(
        "--skip-merged-upload",
        action="store_true",
        help="Skip uploading the merged CSV to Azure Blob Storage.",
    )
    parser.add_argument(
        "--test-db",
        action="store_true",
        help="Attempt a SQL database connection using SQL_CONNECTION_STRING.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)

    config = build_config(args)

    if config.sql_connection_string:
        test_database_connection(config.sql_connection_string)

    try:
        output_path = run_pipeline(
            config,
            upload_raw=not args.skip_raw_upload,
            upload_merged=not args.skip_merged_upload,
        )
        logger.info("Pipeline completed successfully. Output: %s", output_path)
        return 0
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
