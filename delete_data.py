import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog

def delete_data():
    print("Deleting record where ID=3 (Copy-On-Write)...")
    
    try:
        # 1. Load Table and Data
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
        
        current_data = table.scan().to_arrow()
        print(f"Current rows: {current_data.num_rows}")
        
        # 2. Filter data (Keep everything except ID=3)
        # We use PyArrow compute for efficiency
        mask = pc.not_equal(current_data['id'], 3)
        new_data = current_data.filter(mask)
        
        if new_data.num_rows == current_data.num_rows:
            print("[WARN] No records found with ID=3 to delete.")
        else:
            # 3. Overwrite the table
            # This creates a new snapshot with the filtered data
            table.overwrite(new_data)
            print(f"[SUCCESS] Deleted. New rows: {new_data.num_rows}")
            print(f"[VERIFY] Snapshot Summary: {table.current_snapshot().summary}")

    except Exception as e:
        print(f"[FAILED] Delete failed: {e}")

if __name__ == "__main__":
    delete_data()
