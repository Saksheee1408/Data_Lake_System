import pyarrow as pa
from pyiceberg.catalog import load_catalog
from datetime import date

def ingest_data():
    print("Ingesting data via PyIceberg...")
    
    try:
        # 1. Load Catalog
        catalog = load_catalog("default", **{
            "type": "sql",
            "uri": "sqlite:///iceberg_catalog.db",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.region": "us-east-1",
            "warehouse": "s3://warehouse",
        })
        
        # 2. Load Table
        table = catalog.load_table("sales")
        
        # 3. Create Data (PyArrow)
        data = pa.Table.from_pylist([
            {"id": 1, "amount": 100.50, "transaction_date": date(2023, 1, 1)},
            {"id": 2, "amount": 250.00, "transaction_date": date(2023, 1, 2)},
            {"id": 3, "amount": 50.75,  "transaction_date": date(2023, 1, 3)},
        ])
        
        # 4. Append Data
        table.append(data)
        print("[SUCCESS] Data appended to 'sales' table.")
        
        # 5. Verify (Quick count check via metadata)
        # Note: accurate count might need scan, but we'll trust the append for now.
        print(f"[VERIFY] Snapshot Summary: {table.current_snapshot().summary}")
        
    except Exception as e:
        print(f"[FAILED] Data ingestion failed: {e}")

if __name__ == "__main__":
    ingest_data()
