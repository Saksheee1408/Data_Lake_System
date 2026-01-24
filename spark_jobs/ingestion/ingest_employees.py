import sys

# Ensure /app is in the path
if "/app" not in sys.path:
    sys.path.append("/app")

from spark_jobs.common.spark_session import create_spark_session
from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType, DateType
from datetime import date
from decimal import Decimal

def ingest_employee_data():
    spark = create_spark_session("IcebergEmployeeIngestion")
    print("Spark Session created.")

    # Define Schema for Employee
    schema = StructType([
        StructField("id", LongType(), False),
        StructField("name", StringType(), False),
        StructField("department", StringType(), True),
        StructField("salary", DecimalType(10, 2), True),
        StructField("join_date", DateType(), True)
    ])

    # Create Sample Data
    data = [
        (1, "Alice Smith", "Engineering", Decimal("95000.00"), date(2022, 3, 15)),
        (2, "Bob Johnson", "Marketing", Decimal("78000.50"), date(2021, 6, 1)),
        (3, "Charlie Brown", "Sales", Decimal("82000.00"), date(2023, 1, 10)),
    ]

    print("Creating DataFrame...")
    df = spark.createDataFrame(data, schema)
    
    print("Data Preview:")
    df.show()

    # Create/Append to Iceberg Table
    table_name = "local.default.employees"
    
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
             
        print("[SUCCESS] Employee data ingested successfully.")
        
    except Exception as e:
        print(f"[FAILED] Ingestion failed: {e}")

    spark.stop()

if __name__ == "__main__":
    ingest_employee_data()
