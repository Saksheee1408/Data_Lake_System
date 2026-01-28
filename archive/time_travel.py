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
            
        if not history:
            print("[WARN] No history found!")
            return

        # 2. Travel to the FIRST Metadata Version (Original Data)
        first_snapshot_id = history[0].snapshot_id
        print(f"\nTravel Target: Snapshot ID {first_snapshot_id} (The Beginning)")
        
        con = get_duckdb_connection()
        current_loc = table.metadata_location
        
        print(f"Querying table AS OF Snapshot {first_snapshot_id}...")
        
        # FIX: Use 'version' parameter instead of 'snapshot'
        # The version should be the snapshot ID as a string or integer depending on exact build,
        # but usually stringified ID works for 'version'.
        query = f"""
        SELECT * FROM iceberg_scan('{current_loc}', version='{first_snapshot_id}')
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
