from db_connection import get_duckdb_connection
from pyiceberg.catalog import load_catalog
from datetime import datetime

def time_travel():
    print("Time Travel Demo: Exploring History...")
    
    try:
        # 1. Load Table History
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
        
        print("\n--- Table History ---")
        history = table.history()
        for log in history:
            ts = datetime.fromtimestamp(log.timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
            print(f"Snapshot ID: {log.snapshot_id} | Time: {ts}")
            
        # 2. Travel to the FIRST Metadata Version (Original Data)
        # We want to see the table BEFORE we did deletes/updates.
        # We can find the metadata file location for a specific snapshot.
        
        # Let's pick the oldest snapshot
        first_snapshot_id = history[0].snapshot_id
        print(f"\nTravel Target: Snapshot ID {first_snapshot_id} (The Beginning)")
        
        # We need to find the specific metadata file for this snapshot.
        # PyIceberg doesn't give a direct "metadata file path for snapshot X" easily in the high-level API 
        # for external engines without reloading the table state, but we can cheat:
        # We simply tell DuckDB to query the table using the `snapshot_id` option!
        
        con = get_duckdb_connection()
        
        # Get current metadata location to point DuckDB to the table ROOT
        # DuckDB needs the base metadata file, and we pass the snapshot_id as an arg
        current_loc = table.metadata_location
        
        print(f"Querying table AS OF Snapshot {first_snapshot_id}...")
        
        # DuckDB Iceberg Scan with Snapshot ID
        query = f"""
        SELECT * FROM iceberg_scan('{current_loc}', snapshot={first_snapshot_id})
        """
        
        results = con.execute(query).fetchall()
        
        print(f"\n[SUCCESS] Results from the PAST ({len(results)} rows):")
        print("-" * 50)
        print(f"{'ID':<5} | {'Amount':<15} | {'Date':<12}")
        print("-" * 50)
        
        for row in results:
            print(f"{row[0]:<5} | {row[1]:<15} | {row[2]}")

        print("-" * 50)
        print("Notice: ID 3 should be here (even if deleted later).")
        print("Notice: ID 1 should be 100.50 (not 999.99).")

    except Exception as e:
        print(f"[FAILED] Time travel failed: {e}")

if __name__ == "__main__":
    time_travel()
