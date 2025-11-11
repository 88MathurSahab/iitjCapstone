import pandas as pd
import requests
import pyodbc
from datetime import datetime
from meteostat import Hourly, Stations
from azure.storage.blob import BlobServiceClient
import io


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

# --- 1. Azure Blob Storage Configuration ---
# !! IMPORTANT: Paste your Azure Storage Connection String here !!
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
    
    # Check the data types of the indexes
    # Both must be DatetimeIndex for this to work
    print(f"Power data index type: {type(df_hourly.index)}")
    print(f"Weather data index type: {type(df_weather.index)}")

    # Perform an inner merge on the DataFrame indexes.
    # left_index=True and right_index=True tells pandas to
    # use the indexes from both DataFrames as the join key.
    df_merged = pd.merge(
        df_hourly, 
        df_weather, 
        left_index=True, 
        right_index=True, 
        how='inner'
    )

    print("--- Merge Successful ---")
    print(df_merged.head(30))
    
    # This df_merged is now your final, clean, and aligned dataset
    # ready for the database and for feature engineering.

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
