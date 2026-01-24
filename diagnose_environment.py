import sys
import os

try:
    import pyspark
    print(f"PySpark Version: {pyspark.__version__}")
except ImportError:
    print("PySpark not installed.")

from pyspark.sql import SparkSession

def check_environment():
    print("Initializing basic Spark Session to check Scala version...")
    try:
        spark = SparkSession.builder \
            .appName("Diagnostic") \
            .master("local[*]") \
            .getOrCreate()
        
        print(f"Spark Version: {spark.version}")
        
        # Get Scala version from JVM
        scala_version = spark.sparkContext._jvm.scala.util.Properties.versionString()
        print(f"Scala Version: {scala_version}")
        
        spark.stop()
    except Exception as e:
        print(f"Failed to initialize Spark: {e}")

if __name__ == "__main__":
    check_environment()
