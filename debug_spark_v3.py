from pyspark.sql import SparkSession
import os
import sys

def debug_spark_v3():
    endpoint = "http://127.0.0.1:9000"
    print(f"Testing Spark S3 connection to: {endpoint}")
    
    spark = SparkSession.builder \
        .appName("DebugSparkS3_v3") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    try:
        sc = spark.sparkContext
        
        # Check if config is actually set in Hadoop Conf
        hadoop_conf = sc._jsc.hadoopConfiguration()
        access_key = hadoop_conf.get("fs.s3a.access.key")
        print(f"DEBUG: fs.s3a.access.key from Hadoop Conf: {access_key}")
        
        if not access_key:
            print("WARNING: Access key not found in Hadoop Conf. Setting manually...")
            hadoop_conf.set("fs.s3a.access.key", "minioadmin")
            hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
            hadoop_conf.set("fs.s3a.endpoint", endpoint)
            hadoop_conf.set("fs.s3a.path.style.access", "true")
            hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

        # Configuration for embedded credentials
        # s3a://accessKey:secretKey@endpoint/bucket/
        # Note: s3a doesn't always support host:port in authority with credentials easily without encoding.
        # So we stick to standard approach but relies on the manual set above.

        path = "s3a://warehouse/"
        print(f"Attempting to list path: {path}")
        
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(path), hadoop_conf)
        status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(path))
        
        print(f"Successfully listed {len(status)} items in bucket.")
        for s in status:
            print(f" - {s.getPath().toString()}")
            
    except Exception as e:
        print(f"Spark S3 Connection Failed: {e}")
        # import traceback
        # traceback.print_exc()
        
    finally:
        spark.stop()

if __name__ == "__main__":
    debug_spark_v3()
