import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from datetime import date
from decimal import Decimal

def update_data():
    print("Updating ID=1: Changing Amount from 100.50 to 999.99...")
    
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
        
        # 2. Define New Data for ID 1
        arrow_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("amount", pa.decimal128(10, 2), nullable=True),
            pa.field("transaction_date", pa.date32(), nullable=True)
        ])
        
        new_data = pa.Table.from_pylist([
            {"id": 1, "amount": Decimal("999.99"), "transaction_date": date(2023, 1, 1)}
        ], schema=arrow_schema)
        
        # 3. Execute ACID Transaction
        # We delete the old record and append the new one in a single transaction context
        with table.transaction() as txn:
            txn.delete(where=EqualTo("id", 1))
            txn.append(new_data)
            
        print("[SUCCESS] Updated ID 1 (Atomic Delete + Append).")
        print(f"[VERIFY] Snapshot Summary: {table.current_snapshot().summary}")
        
    except Exception as e:
        print(f"[FAILED] Update failed: {e}")

if __name__ == "__main__":
    update_data()
