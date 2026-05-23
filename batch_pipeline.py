"""
Automated Batch Pipeline for Nightly Data Ingestion
Runs the complete pipeline: data loading, ingestion, and feature engineering.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from azure.storage.blob import BlobServiceClient

from db_schema import create_time_series_schema, get_schema_info
from feature_engineering import run_all_feature_engineering
from ingestion_pipeline import (
    check_existing_data,
    ingest_data_streaming,
    load_data_from_azure,
    load_data_from_csv,
    prepare_data_for_db,
    upsert_data,
)

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


def configure_logging(verbose: bool) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


def run_batch_pipeline(
    connection_string: str,
    data_source: str,
    source_type: str = "csv",
    azure_connection_string: Optional[str] = None,
    container_name: Optional[str] = None,
    create_schema: bool = False,
    run_feature_engineering: bool = True,
    batch_size: int = 1000,
    upsert_mode: bool = True,
) -> dict:
    """
    Run the complete batch pipeline.
    
    Args:
        connection_string: Database connection string
        data_source: Path to CSV or blob name
        source_type: 'csv' or 'azure'
        azure_connection_string: Azure storage connection string
        container_name: Azure container name
        create_schema: Whether to create database schema
        run_feature_engineering: Whether to run feature engineering
        batch_size: Batch size for ingestion
        upsert_mode: Use upsert instead of insert
    
    Returns:
        Dictionary with pipeline results
    """
    results = {
        "schema_created": False,
        "rows_ingested": 0,
        "feature_engineering": {},
        "errors": []
    }
    
    try:
        # Step 1: Create schema if needed
        if create_schema:
            logger.info("Creating database schema...")
            create_time_series_schema(connection_string)
            results["schema_created"] = True
            logger.info("Schema created successfully")
        
        # Step 2: Check existing data
        existing_data = check_existing_data(connection_string)
        if existing_data.get("exists"):
            logger.info(
                "Existing data found: %d rows from %s to %s",
                existing_data["row_count"],
                existing_data.get("min_date"),
                existing_data.get("max_date"),
            )
        
        # Step 3: Load data
        logger.info("Loading data from %s: %s", source_type, data_source)
        if source_type == "csv":
            df = load_data_from_csv(Path(data_source))
        elif source_type == "azure":
            if not azure_connection_string or not container_name:
                raise ValueError("azure_connection_string and container_name required for Azure source")
            df = load_data_from_azure(azure_connection_string, container_name, data_source)
        else:
            raise ValueError(f"Unknown source_type: {source_type}")
        
        # Step 4: Ingest data
        logger.info("Ingesting data into database...")
        if upsert_mode:
            rows_ingested = upsert_data(connection_string, df, batch_size)
        else:
            from ingestion_pipeline import ingest_data_batch
            rows_ingested = ingest_data_batch(connection_string, df, batch_size)
        
        results["rows_ingested"] = rows_ingested
        logger.info("Ingested %d rows", rows_ingested)
        
        # Step 5: Run feature engineering
        if run_feature_engineering:
            logger.info("Running feature engineering...")
            feature_results = run_all_feature_engineering(connection_string)
            results["feature_engineering"] = feature_results
            logger.info("Feature engineering completed")
        
        # Step 6: Get schema info
        schema_info = get_schema_info(connection_string)
        results["schema_info"] = schema_info
        
        logger.info("Batch pipeline completed successfully")
        
    except Exception as exc:
        logger.error("Batch pipeline failed: %s", exc, exc_info=True)
        results["errors"].append(str(exc))
        raise
    
    return results


def main(argv: Optional[list[str]] = None) -> int:
    """Main entry point for batch pipeline."""
    parser = argparse.ArgumentParser(
        description="Automated batch pipeline for data ingestion and feature engineering."
    )
    
    parser.add_argument(
        "--data-source",
        required=True,
        help="Path to CSV file or Azure blob name",
    )
    parser.add_argument(
        "--source-type",
        choices=["csv", "azure"],
        default="csv",
        help="Type of data source (default: csv)",
    )
    parser.add_argument(
        "--sql-connection",
        default=os.getenv("SQL_CONNECTION_STRING", DEFAULT_SQL_CONNECTION_STRING),
        help="SQL Server connection string",
    )
    parser.add_argument(
        "--azure-connection",
        default=os.getenv("AZURE_CONNECTION_STRING", DEFAULT_AZURE_CONNECTION),
        help="Azure Storage connection string",
    )
    parser.add_argument(
        "--container-name",
        default=os.getenv("AZURE_CONTAINER_NAME", "greenpowerstorage-container"),
        help="Azure container name",
    )
    parser.add_argument(
        "--create-schema",
        action="store_true",
        help="Create database schema before ingestion",
    )
    parser.add_argument(
        "--skip-features",
        action="store_true",
        help="Skip feature engineering step",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for ingestion (default: 1000)",
    )
    parser.add_argument(
        "--insert-only",
        action="store_true",
        help="Use insert instead of upsert (may fail on duplicates)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    
    args = parser.parse_args(argv)
    configure_logging(args.verbose)
    
    try:
        results = run_batch_pipeline(
            connection_string=args.sql_connection,
            data_source=args.data_source,
            source_type=args.source_type,
            azure_connection_string=args.azure_connection if args.source_type == "azure" else None,
            container_name=args.container_name if args.source_type == "azure" else None,
            create_schema=args.create_schema,
            run_feature_engineering=not args.skip_features,
            batch_size=args.batch_size,
            upsert_mode=not args.insert_only,
        )
        
        logger.info("Pipeline Results:")
        logger.info("  Schema created: %s", results["schema_created"])
        logger.info("  Rows ingested: %d", results["rows_ingested"])
        logger.info("  Feature engineering: %s", results.get("feature_engineering", {}))
        
        if results.get("schema_info"):
            logger.info("  Tables: %d", len(results["schema_info"].get("tables", [])))
            logger.info("  Partitions: %d", len(results["schema_info"].get("partitions", [])))
        
        return 0
        
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

