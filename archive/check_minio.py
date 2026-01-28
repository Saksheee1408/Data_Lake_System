from minio import Minio
from minio.error import S3Error
import os
import sys

def check_minio():
    endpoint = os.environ.get("S3_ENDPOINT", "localhost:9000")
    # Strip http:// prefix if present, as Minio client expects just host:port or uses secure=False
    if endpoint.startswith("http://"):
        endpoint = endpoint.replace("http://", "")
        secure = False
    elif endpoint.startswith("https://"):
        endpoint = endpoint.replace("https://", "")
        secure = True
    else:
        secure = False # Default assumption

    print(f"Connecting to MinIO at {endpoint} (Secure: {secure})...")
    
    client = Minio(
        endpoint,
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=secure
    )

    try:
        # List buckets
        buckets = client.list_buckets()
        print("Connected successfully!")
        print(f"Found {len(buckets)} buckets.")
        
        bucket_names = [b.name for b in buckets]
        for b in bucket_names:
            print(f" - {b}")
            
        target_bucket = "warehouse"
        if target_bucket not in bucket_names:
            print(f"Bucket '{target_bucket}' not found. Creating...")
            client.make_bucket(target_bucket)
            print(f"Bucket '{target_bucket}' created.")
        else:
            print(f"Bucket '{target_bucket}' exists.")
            
    except S3Error as e:
        print(f"MinIO Access Error: {e}")
    except Exception as e:
        print(f"Connection Failed: {e}")
        print("Please check if MinIO container is running and port 9000 is exposed.")

if __name__ == "__main__":
    check_minio()
