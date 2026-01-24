import argparse
import sys
import os
from pyspark.sql import SparkSession

def create_spark_session(app_name):
    # Hardcode endpoint to 127.0.0.1 for reliability from host
    s3_endpoint = "http://127.0.0.1:9000"
    
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/iceberg_warehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def read_table(table_name, limit=20):
    spark = create_spark_session(f"ReadTable_{table_name}")
    
    print(f"Reading from Iceberg table: {table_name}")
    
    try:
        if not spark.catalog.tableExists(table_name):
            print(f"ERROR: Table '{table_name}' does not exist.")
            return

        df = spark.read.table(table_name)
        
        count = df.count()
        print(f"Total Record Count: {count}")
        
        print(f"Showing top {limit} rows:")
        df.show(limit, truncate=False)
        
    except Exception as e:
        print(f"FAILED: Error reading table: {e}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read Iceberg Table")
    parser.add_argument("--table", required=True, help="Iceberg table name (e.g. local.default.leads)")
    parser.add_argument("--limit", type=int, default=20, help="Number of rows to show")
    
    args = parser.parse_args()
    
    read_table(args.table, args.limit)
