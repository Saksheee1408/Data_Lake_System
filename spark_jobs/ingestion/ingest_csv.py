import argparse
import sys
import os

# Add the project root directory to sys.path
# This allows imports like 'from spark_jobs...' to work regardless of where the script is run from
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
# Fix: The logic above for project_root might be wrong depending on nesting.
# ingest_csv.py is in spark_jobs/ingestion (2 levels deep from spark_jobs, 3 from root)
# Let's simple use relative path logic safer.
# d:\DataLake2\spark_jobs\ingestion\ingest_csv.py
# dirname -> ingestion
# dirname -> spark_jobs
# dirname -> DataLake2

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir) # spark_jobs
grandparent_dir = os.path.dirname(parent_dir) # DataLake2

if grandparent_dir not in sys.path:
    sys.path.append(grandparent_dir)

# Helper for Docker execution
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
                 df.writeTo(table_name).overwritePartitions() 
            else:
                 df.writeTo(table_name).append()
        else:
            print(f"Table {table_name} does not exist. Creating...")
            df.writeTo(table_name).create()
            
        print("[SUCCESS] Data ingested successfully.")
        
    except Exception as e:
        print(f"[FAILED] Ingestion failed: {e}")
        # raise e 
        
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV into Iceberg")
    parser.add_argument("--file", required=True, help="Path to the CSV file")
    parser.add_argument("--table", required=True, help="Target Iceberg table name (e.g. local.default.leads)")
    parser.add_argument("--mode", default="append", choices=["append", "overwrite"], help="Write mode")
    
    args = parser.parse_args()
    
    ingest_csv(args.file, args.table, args.mode)
