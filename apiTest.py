<<<<<<< HEAD
import pandas as pd
import requests
import pyodbc
from datetime import datetime
from meteostat import Hourly, Stations
from azure.storage.blob import BlobServiceClient
import io
import zipfile
import os


try:
    server = 'tcp:greenpowerutilities-dbserver.database.windows.net,1433'
    database = 'GreenPowerUtilities_DB'
    
    conn_str = (
        'DRIVER={ODBC Driver 18 for SQL Server};'
        'SERVER=' + server + ';'
        'DATABASE=' + database + ';'
        'Uid=CloudSAe9b21290;'
        'Pwd=Yash@8896;'
        'Encrypt=yes;'
        'TrustServerCertificate=no;'
        'Connection Timeout=30;'
    )
    cnxn = pyodbc.connect(conn_str)
    print('connected successfully')
    print('DB connection object --> ', cnxn)

except Exception as e:
    print('failed at db connection')
    print(e)


# --- 1. Configuration ---
# Source URL for the ZIP file
# Updated URL: UCI ML repository was restructured; using the direct download link
ZIP_URL = "https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip"
# The name of the unzipped file you want to upload
FILE_TO_EXTRACT = "household_power_consumption.txt" 

# Azure Blob Storage Configuration
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=greenpowerstorage;AccountKey=GNvSIF/x7DFrDBBDiuyRVBRjGBs3J41+TNnUBjiiO0Pj/BImCvK7Bp5fCnzmYw+RV5ucKtrT17D4+AStt6eZ5Q==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "greenpowerstorage-container"
BLOB_NAME = FILE_TO_EXTRACT # The final name of the blob in Azure

# --- 2. Download the ZIP File into Memory ---
print(f"Downloading ZIP file from: {ZIP_URL}")
try:
    response = requests.get(ZIP_URL, stream=True)
    response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
    
    # Use io.BytesIO to hold the downloaded zip content in memory
    zip_in_memory = io.BytesIO(response.content)
    print("Download complete. Content stored in memory.")

except requests.exceptions.RequestException as e:
    print(f"Error during download: {e}")
    exit()

unzipped_content = None
print(f"Attempting to extract: {FILE_TO_EXTRACT}")

try:
    with zipfile.ZipFile(zip_in_memory, 'r') as zf:
        if FILE_TO_EXTRACT in zf.namelist():
            unzipped_content = zf.read(FILE_TO_EXTRACT)
            print(f"Successfully extracted {FILE_TO_EXTRACT} content.")
        else:
            print(f"Error: {FILE_TO_EXTRACT} not found in the ZIP archive.")
            print(f"Files found in ZIP: {zf.namelist()}")
            exit()
except Exception as e:
    print(f"Error during unzipping: {e}")
    exit()

# --- 4. Upload the Unzipped Content to Azure Blob Storage ---
print(f"Connecting to Azure Blob Storage container: {CONTAINER_NAME}")

try:
    # Create the BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    
    # Create a BlobClient for the target file
    blob_client = container_client.get_blob_client(BLOB_NAME)

    # Upload the content
    print(f"Uploading content to blob: {BLOB_NAME}")
    blob_client.upload_blob(unzipped_content, overwrite=True)
    
    print("\n✅ Success!")
    print(f"File uploaded to Azure Blob Storage as: {BLOB_NAME}")

except Exception as e:
    print(f"Error during Azure upload: {e}")

# --- 1. Azure Blob Storage Configuration ---
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=greenpowerstorage;AccountKey=GNvSIF/x7DFrDBBDiuyRVBRjGBs3J41+TNnUBjiiO0Pj/BImCvK7Bp5fCnzmYw+RV5ucKtrT17D4+AStt6eZ5Q==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "greenpowerstorage-container" 
BLOB_NAME = "household_power_consumption.txt" 

print(f"Loading data from Azure Blob Storage: {CONTAINER_NAME}/{BLOB_NAME}...")

file_path = "household_power_consumption.txt"
print(f"Loading data from {file_path}...")

try:
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
    
    blob_data = blob_client.download_blob().readall()
    
    data_stream = io.StringIO(blob_data.decode('utf-8'))
    print("File downloaded from Azure. Now parsing with pandas...")

    df = pd.read_csv(
        data_stream,
        sep=';',
        na_values=['?'],
        parse_dates={'DateTime': ['Date', 'Time']},
        dayfirst=True, 
        index_col='DateTime' 
    )

    print("--- File Loaded Successfully ---")

    numeric_columns = [
        'Global_active_power',
        'Global_reactive_power',
        'Voltage',
        'Global_intensity',
        'Sub_metering_1',
        'Sub_metering_2',
        'Sub_metering_3'
    ]

    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)

    print("\n--- DataFrame is ready for analysis ---")
    # df.head(20)

except FileNotFoundError:
    print(f"Error: The file '{file_path}' was not found.")
except Exception as e:
    print(f"An error occurred: {e}")

print("\nResampling data from minute-to-hourly frequency...")

try:
    aggregation_rules = {
        'Global_active_power': 'mean',
        'Global_reactive_power': 'mean',
        'Voltage': 'mean',
        'Global_intensity': 'mean',
        'Sub_metering_1': 'sum',
        'Sub_metering_2': 'sum',
        'Sub_metering_3': 'sum'
    }

    df_hourly = df.resample('h').agg(aggregation_rules)

    print("--- Resampling Complete ---")
    print(df_hourly.head(30))

except NameError:
    print("\nError: The DataFrame 'df' was not loaded.")
    print("Resampling can only happen after the data is loaded from blob storage.")
except Exception as e:
    print(f"An error occurred during resampling: {e}")  

start = datetime(2006, 12, 16)
end = datetime(2010, 11, 26, 23, 59) 

# Paris-Orly
station_id = '07149'

print(f"Fetching data for station {station_id} from {start} to {end}...")

data = Hourly(station_id, start, end)
df_weather = data.fetch()

print("\n--- Weather Data Fetched Successfully ---")
print(df_weather.head(30))

try:
    print("\n--- Merging hourly power data and weather data ---")

    df_merged = pd.merge(
        df_hourly, 
        df_weather, 
        left_index=True, 
        right_index=True, 
        how='inner'
    )

    print("--- Merge Successful ---")
    print(df_merged.head(30))
    
    cols_to_drop = ['snow', 'wpgt', 'tsun', 'coco']
    df_merged.drop(columns=cols_to_drop, inplace=True)
    print(" Dropped useless columns.")

    df_merged['prcp'].fillna(0, inplace=True)
    print(" Filled missing 'prcp' with 0.")

    df_merged.interpolate(method='time', inplace=True)
    print("Performed time-based interpolation on all remaining gaps.")

    # Create new time-based features
    df_merged['hour_of_day'] = df_merged.index.hour
    df_merged['day_of_week'] = df_merged.index.dayofweek  # 0=Monday, 6=Sunday
    df_merged['month_of_year'] = df_merged.index.month
    df_merged['is_weekend'] = df_merged['day_of_week'].isin([5, 6]).astype(int) # 1 if True, 0 if False
    print("Step 4: Engineered time-based features (hour, day, month, weekend).")

    # Create lag features
    df_merged['lag_power_1h'] = df_merged['Global_active_power'].shift(1)
    df_merged['lag_power_24h'] = df_merged['Global_active_power'].shift(24)
    df_merged['lag_temp_1h'] = df_merged['temp'].shift(1)

    df.dropna(inplace=True)
    print("\n Dropped initial NaN rows. Data is clean and ready.") 

    print("\n--- Final Processed Data (Ready for Database) ---")
    print(df_merged.head())

except NameError:
    print("\nError: 'df_hourly' or 'df_weather' not found.")
    print("Make sure both DataFrames are loaded and prepared before merging.")
except Exception as e:
    print(f"An error occurred during merging: {e}")

try:
    output_csv_path = "merged_power_and_weather.csv"
    
    print(f"\nSaving the final merged DataFrame to {output_csv_path}...")

    df_merged.to_csv(
        output_csv_path,
        encoding='utf-8',   
        sep=',',            
        index=True,         
        index_label='DateTime' 
    )
    
    print(f"--- Successfully saved file to {output_csv_path} ---")

except NameError:
    print("\nError: The 'df_merged' DataFrame was not found.")
    print("Please ensure the merging step ran successfully before saving.")
except Exception as e:
    print(f"An error occurred while saving the file: {e}")
=======
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
    processed_blob_name: str
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


def clean_data(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and preprocess the merged dataset by:
    - Dropping useless columns
    - Filling missing values
    - Performing time-based interpolation
    - Creating time-based features
    - Creating lag features
    """
    logger.info("Starting data cleaning and preprocessing")

    # Drop useless columns
    cols_to_drop = ["snow", "wpgt", "tsun", "coco"]
    # Only drop columns that exist in the dataframe
    cols_to_drop = [col for col in cols_to_drop if col in df_merged.columns]
    if cols_to_drop:
        df_merged.drop(columns=cols_to_drop, inplace=True)
        logger.info("Dropped useless columns: %s", cols_to_drop)

    # Fill missing 'prcp' with 0
    if "prcp" in df_merged.columns:
        df_merged["prcp"].fillna(0, inplace=True)
        logger.info("Filled missing 'prcp' with 0")

    # Perform time-based interpolation on all remaining gaps
    df_merged.interpolate(method="time", inplace=True)
    logger.info("Performed time-based interpolation on all remaining gaps")

    # Create new time-based features
    df_merged["hour_of_day"] = df_merged.index.hour
    df_merged["day_of_week"] = df_merged.index.dayofweek  # 0=Monday, 6=Sunday
    df_merged["month_of_year"] = df_merged.index.month
    df_merged["is_weekend"] = (
        df_merged["day_of_week"].isin([5, 6]).astype(int)
    )  # 1 if True, 0 if False
    logger.info("Engineered time-based features (hour, day, month, weekend)")

    # Create lag features
    if "Global_active_power" in df_merged.columns:
        df_merged["lag_power_1h"] = df_merged["Global_active_power"].shift(1)
        df_merged["lag_power_24h"] = df_merged["Global_active_power"].shift(24)

    if "temp" in df_merged.columns:
        df_merged["lag_temp_1h"] = df_merged["temp"].shift(1)

    # Drop initial NaN rows created by lag features
    df_merged.dropna(inplace=True)
    logger.info("Dropped initial NaN rows. Data is clean and ready")

    logger.debug("Final processed data shape: %s", df_merged.shape)
    logger.debug("Final processed data head:\n%s", df_merged.head())

    return df_merged


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
        f"{raw_prefix}/merged_power_and_weather.csv"
        if raw_prefix
        else "merged_power_and_weather.csv"
    )
    processed_blob_name = (
        f"{processed_prefix}/processed_power_and_weather.csv"
        if processed_prefix
        else "processed_power_and_weather.csv"
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
        processed_blob_name=processed_blob_name,
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

    # Save merged data (before cleaning)
    save_dataframe(merged_df, config.output_path)

    if upload_merged and blob_container is not None:
        upload_blob_file(blob_container, config.merged_blob_name, config.output_path)

    # Clean and process the data
    processed_df = clean_data(merged_df.copy())

    # Save processed data to current directory
    processed_output_path = Path("processed_power_and_weather.csv")
    save_dataframe(processed_df, processed_output_path)
    logger.info("Saved processed data to %s", processed_output_path.absolute())

    # Upload processed data to processed container
    if upload_merged and blob_container is not None:
        try:
            upload_blob_file(
                blob_container, config.processed_blob_name, processed_output_path
            )
            logger.info(
                "Successfully uploaded processed data to %s in container %s",
                config.processed_blob_name,
                config.container_name,
            )
        except Exception as exc:
            logger.error(
                "Failed to upload processed data to Azure: %s", exc, exc_info=True
            )
            logger.warning(
                "Processed data saved locally at %s but not uploaded to Azure",
                processed_output_path.absolute(),
            )

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
>>>>>>> origin/dev_javed
