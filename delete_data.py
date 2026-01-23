from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

def delete_data():
    print("Deleting record where ID=3...")
    
    try:
        # 1. Load Table
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
        
        # 2. Perform Delete
        # We delete rows where id == 3
        # PyIceberg supports boolean expressions for delete
        table.delete(where=EqualTo("id", 3))
        
        print("[SUCCESS] Deleted record with ID 3.")
        print(f"[VERIFY] Snapshot Summary: {table.current_snapshot().summary}")
        
    except Exception as e:
        print(f"[FAILED] Delete failed: {e}")

if __name__ == "__main__":
    delete_data()
