from db_connection import get_duckdb_connection
from pyiceberg.catalog import load_catalog

def read_data():
    print("Reading data from 'default.sales'...")
    
    try:
        # 1. Ask PyIceberg where the current metadata is
        # We need this because DuckDB doesn't know about our local sqlite catalog
        catalog = load_catalog("default", **{
            "type": "sql",
            "uri": "sqlite:///iceberg_catalog.db",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.region": "us-east-1",
            "warehouse": "s3://warehouse",
        })
        
        table = catalog.load_table("default.sales")
        metadata_auth_location = table.metadata_location
        
        # Strip "s3://" prefix if present and handle S3 path style for DuckDB if needed
        # But usually DuckDB handles the s3:// path fine if configured.
        print(f"Current Metadata Location: {metadata_auth_location}")
        
        # 2. Query using DuckDB with valid Metadata File
        con = get_duckdb_connection()
        
        query = f"SELECT * FROM iceberg_scan('{metadata_auth_location}')"
        
        results = con.execute(query).fetchall()
        
        print(f"\n[SUCCESS] Found {len(results)} records:")
        print("-" * 50)
        print(f"{'ID':<5} | {'Amount':<15} | {'Date':<12}")
        print("-" * 50)
        
        for row in results:
            print(f"{row[0]:<5} | {row[1]:<15} | {row[2]}")
            
    except Exception as e:
        print(f"[FAILED] Could not read data: {e}")

if __name__ == "__main__":
    read_data()
