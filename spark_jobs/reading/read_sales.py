import sys

# Ensure /app is in the path
if "/app" not in sys.path:
    sys.path.append("/app")

from spark_jobs.common.spark_session import create_spark_session

def read_data():
    spark = create_spark_session("IcebergSalesRead")
    print("Reading data from 'local.default.sales'...")
    
    try:
        df = spark.table("local.default.sales")
        print(f"\n[SUCCESS] Found {df.count()} records:")
        df.show()
    except Exception as e:
        print(f"[FAILED] Could not read data: {e}")

    spark.stop()

if __name__ == "__main__":
    read_data()
