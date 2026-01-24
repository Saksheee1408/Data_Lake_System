import argparse
import sys

# Ensure /app is in the path
if "/app" not in sys.path:
    sys.path.append("/app")

from spark_jobs.common.spark_session import create_spark_session

def read_table(table_name, limit=20):
    spark = create_spark_session(f"ReadTable_{table_name}")
    print(f"Reading from table: {table_name}")
    
    try:
        if not spark.catalog.tableExists(table_name):
            print(f"[ERROR] Table {table_name} does not exist.")
            return

        df = spark.table(table_name)
        count = df.count()
        print(f"[SUCCESS] Table has {count} records.")
        
        print("Schema:")
        df.printSchema()
        
        print(f"Showing top {limit} rows:")
        df.show(limit, truncate=False)
        
    except Exception as e:
        print(f"[FAILED] Read failed: {e}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read Iceberg Table")
    parser.add_argument("--table", required=True, help="Iceberg table name")
    parser.add_argument("--limit", type=int, default=20, help="Number of rows to show")
    
    args = parser.parse_args()
    read_table(args.table, args.limit)
