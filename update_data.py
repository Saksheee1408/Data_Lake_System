import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog
from datetime import date
from decimal import Decimal

def update_data():
    print("Updating ID=1: Changing Amount from 100.50 to 999.99 (Copy-On-Write)...")
    
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
        
        # 2. Get Current Data and Remove Old Record (ID=1)
        current_data = table.scan().to_arrow()
        mask = pc.not_equal(current_data['id'], 1)
        kept_data = current_data.filter(mask)
        
        # 3. Create New Record
        # Ensure schema matches exactly
        arrow_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("amount", pa.decimal128(10, 2), nullable=True),
            pa.field("transaction_date", pa.date32(), nullable=True)
        ])
        
        new_record = pa.Table.from_pylist([
            {"id": 1, "amount": Decimal("999.99"), "transaction_date": date(2023, 1, 1)}
        ], schema=arrow_schema)
        
        # 4. Combine (Union)
        final_data = pa.concat_tables([kept_data, new_record])
        
        # 5. Overwrite
        table.overwrite(final_data)
        
        print("[SUCCESS] Updated ID 1.")
        print(f"[VERIFY] Snapshot Summary: {table.current_snapshot().summary}")
        
    except Exception as e:
        print(f"[FAILED] Update failed: {e}")

if __name__ == "__main__":
    update_data()
