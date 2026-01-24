import argparse
import sys
import os

# Ensure /app is in the path
if "/app" not in sys.path:
    sys.path.append("/app")

from spark_jobs.common.spark_session import create_spark_session

def ingest_csv(file_path, table_name, mode="append"):
    spark = create_spark_session(f"IngestCSV_{os.path.basename(file_path)}")
    
    print(f"Reading CSV from: {file_path}")
    
    try:
        # Read CSV with inferred schema
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        
        print("Schema Inferred:")
        df.printSchema()
        
        print(f"Writing to Iceberg table: {table_name} (Mode: {mode})")
        
        # Create database if it doesn't exist (assuming format local.db.table)
        parts = table_name.split(".")
        if len(parts) >= 2:
            db_name = f"{parts[0]}.{parts[1]}"
            print(f"Ensuring database {db_name} exists...")
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

        if spark.catalog.tableExists(table_name):
            print(f"Table {table_name} exists.")
            if mode == "overwrite":
                 df.writeTo(table_name).overwritePartitions() # or replace()
            else:
                 df.writeTo(table_name).append()
        else:
            print(f"Table {table_name} does not exist. Creating...")
            df.writeTo(table_name).create()
            
        print("[SUCCESS] Data ingested successfully.")
        
    except Exception as e:
        print(f"[FAILED] Ingestion failed: {e}")
        # raise e # Optionally raise to fail the job
        
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV into Iceberg")
    parser.add_argument("--file", required=True, help="Path to the CSV file")
    parser.add_argument("--table", required=True, help="Target Iceberg table name (e.g. local.default.leads)")
    parser.add_argument("--mode", default="append", choices=["append", "overwrite"], help="Write mode")
    
    args = parser.parse_args()
    
    # Resolve absolute path if needed, though Spark might handle relative paths differently depending on context.
    # We'll pass the path as is, assuming user provides a valid path for Spark.
    # If running in Docker, paths might need mapping.
    
    ingest_csv(args.file, args.table, args.mode)
