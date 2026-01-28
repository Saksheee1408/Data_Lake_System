from pyspark.sql import SparkSession
import os
import sys

def debug_spark():
    # Force 127.0.0.1 to avoid localhost resolution issues
    endpoint = "http://127.0.0.1:9000"
    print(f"Testing Spark S3 connection to: {endpoint}")
    
    spark = SparkSession.builder \
        .appName("DebugSparkS3") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    try:
        print("Spark session created.")
        # Try a simple direct read from the bucket using s3a (bypassing Iceberg catalog for a moment)
        # We need to know a valid path. 'warehouse' bucket exists.
        path = "s3a://warehouse/"
        print(f"Attempting to list path: {path}")
        
        # Use Hadoop FileSystem API via SparkContext
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(path), spark._jvm.org.apache.hadoop.conf.Configuration())
        status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(path))
        
        print(f"Successfully listed {len(status)} items in bucket.")
        for s in status:
            print(f" - {s.getPath().toString()}")
            
    except Exception as e:
        print(f"Spark S3 Connection Failed: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()

if __name__ == "__main__":
    debug_spark()
