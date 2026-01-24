import sys

# Ensure /app is in the path
if "/app" not in sys.path:
    sys.path.append("/app")

from spark_jobs.common.spark_session import create_spark_session
from pyspark.sql.types import StructType, StructField, LongType, DecimalType, DateType
from datetime import date
from decimal import Decimal

def ingest_data():
    spark = create_spark_session("IcebergSalesIngestion")
    print("Spark Session created.")

    # Define Schema
    schema = StructType([
        StructField("id", LongType(), False),
        StructField("amount", DecimalType(10, 2), True),
        StructField("transaction_date", DateType(), True)
    ])

    # Create Data
    data = [
        (1, Decimal("100.50"), date(2023, 1, 1)),
        (2, Decimal("250.00"), date(2023, 1, 2)),
        (3, Decimal("50.75"), date(2023, 1, 3)),
    ]

    print("Creating DataFrame...")
    df = spark.createDataFrame(data, schema)
    
    print("Data Preview:")
    df.show()

    # Create/Append to Iceberg Table
    table_name = "local.default.sales"
    
    print(f"Writing to {table_name}...")
    
    try:
        # Create database if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS local.default")
        
        # Check if table exists
        if spark.catalog.tableExists(table_name):
             print("Table exists, appending...")
             df.writeTo(table_name).append()
        else:
             print("Table does not exist, creating...")
             df.writeTo(table_name).create()
             
        print("[SUCCESS] Sales data ingested successfully.")
        
    except Exception as e:
        print(f"[FAILED] Ingestion failed: {e}")

    spark.stop()

if __name__ == "__main__":
    ingest_data()
