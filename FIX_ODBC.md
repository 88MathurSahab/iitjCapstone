# Quick Fix for ODBC Driver Issue

## Immediate Solution

**Option 1: Use the Fixed Installation Script (Recommended)**

```bash
sudo ./install_odbc_fixed.sh
```

**Option 2: Manual Installation (Modern Method - No apt-key)**

```bash
# Install prerequisites
sudo apt-get update
sudo apt-get install -y curl gnupg ca-certificates

# Add Microsoft repository using modern keyring method (no deprecated apt-key)
curl -sSL https://packages.microsoft.com/keys/microsoft.asc | sudo gpg --dearmor | sudo tee /usr/share/keyrings/microsoft-prod.gpg > /dev/null

# Get your Ubuntu version and add repository
UBUNTU_VERSION=$(lsb_release -rs)
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/${UBUNTU_VERSION}/prod ${UBUNTU_VERSION} main" | sudo tee /etc/apt/sources.list.d/mssql-release.list

# Install ODBC Driver
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev

# Verify installation
odbcinst -q -d
```

**Note:** The old `apt-key` method is deprecated. Use the modern keyring method above.

## After Installation

Run the pipeline again:

```bash
python batch_pipeline.py \
    --data-source processed_power_and_weather.csv \
    --source-type csv \
    --create-schema
```

## Alternative: Use Docker (No Local Installation Needed)

If you don't want to install ODBC driver locally, use Docker:

```bash
# Build Docker image (includes ODBC driver)
docker build -t iitj-capstone .

# Run pipeline in Docker
docker run --rm \
    -v $(pwd)/processed_power_and_weather.csv:/app/processed_power_and_weather.csv \
    -e SQL_CONNECTION_STRING="DRIVER={ODBC Driver 18 for SQL Server};SERVER=tcp:greenpowerutilities-dbserver.database.windows.net,1433;DATABASE=GreenPowerUtilities_DB;Uid=CloudSAe9b21290;Pwd=Yash@8896;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;" \
    iitj-capstone \
    python batch_pipeline.py \
        --data-source processed_power_and_weather.csv \
        --source-type csv \
        --create-schema
```

