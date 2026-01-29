import argparse
import sys
import os

# Ensure project root is in path
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
        
        # Determine database name from table name (e.g. default.sales -> default)
        parts = table_name.split(".")
        if len(parts) > 1:
            # Assume last part is table, everything before is namespace/db
            db_name = ".".join(parts[:-1])
            print(f"Ensuring database/namespace '{db_name}' exists...")
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

        if spark.catalog.tableExists(table_name):
            print(f"Table {table_name} exists.")
            if mode == "overwrite":
                 print("Mode is overwrite. Replacing table data.")
                 df.writeTo(table_name).overwritePartitions() 
            else:
                 print("Mode is append. Appending data.")
                 df.writeTo(table_name).append()
        else:
            print(f"Table {table_name} does not exist. Creating...")
            # Use 'using iceberg' to be explicit, though session config defaults might handle it
            df.writeTo(table_name).using("iceberg").create()
            
        print("[SUCCESS] Data ingested successfully.")
        
    except Exception as e:
        print(f"[FAILED] Ingestion failed: {e}")
        # raise e 
        
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV into Iceberg")
    parser.add_argument("--file", required=True, help="Path to the CSV file")
    parser.add_argument("--table", required=True, help="Target Iceberg table name (e.g. default.leads)")
    parser.add_argument("--mode", default="append", choices=["append", "overwrite"], help="Write mode")
    
    args = parser.parse_args()
    
    ingest_csv(args.file, args.table, args.mode)
