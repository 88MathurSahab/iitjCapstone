# 🚀 Quick Fix & Run Guide

## ⚡ Fix ODBC Driver Issue (Run This First!)

**Modern Method (Recommended - Works on Ubuntu 22.04+):**

```bash
# Use the fixed installation script
sudo ./install_odbc_fixed.sh
```

**Or manually (if script doesn't work):**

```bash
# Install prerequisites
sudo apt-get update
sudo apt-get install -y curl gnupg ca-certificates

# Add Microsoft repository (modern method - no apt-key)
curl -sSL https://packages.microsoft.com/keys/microsoft.asc | sudo gpg --dearmor | sudo tee /usr/share/keyrings/microsoft-prod.gpg > /dev/null

# Add repository (replace UBUNTU_VERSION with your version, e.g., 22.04)
UBUNTU_VERSION=$(lsb_release -rs)
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/${UBUNTU_VERSION}/prod ${UBUNTU_VERSION} main" | sudo tee /etc/apt/sources.list.d/mssql-release.list

# Install
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev

# Verify it worked
odbcinst -q -d
```

## ✅ Run Pipeline from Terminal

```bash
python batch_pipeline.py \
    --data-source processed_power_and_weather.csv \
    --source-type csv \
    --create-schema
```

## 🐳 OR Use Docker (No Local Installation)

```bash
# Build image
docker build -t iitj-capstone .

# Run pipeline
docker run --rm \
    -v $(pwd)/processed_power_and_weather.csv:/app/processed_power_and_weather.csv \
    -e SQL_CONNECTION_STRING="DRIVER={ODBC Driver 18 for SQL Server};SERVER=tcp:greenpowerutilities-dbserver.database.windows.net,1433;DATABASE=GreenPowerUtilities_DB;Uid=CloudSAe9b21290;Pwd=Yash@8896;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;" \
    iitj-capstone \
    python batch_pipeline.py \
        --data-source processed_power_and_weather.csv \
        --source-type csv \
        --create-schema
```

## 📚 Full Documentation

- **RUN_GUIDE.md** - Complete terminal and Docker instructions
- **FIX_ODBC.md** - Detailed ODBC driver installation
- **PIPELINE_DOCUMENTATION.md** - Architecture and features
- **QUICKSTART.md** - Quick start examples

