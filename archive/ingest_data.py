import pyarrow as pa
from pyiceberg.catalog import load_catalog
from datetime import date
from decimal import Decimal

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
        table = catalog.load_table("default.sales")
        
        # 3. Define Explicit Arrow Schema (Must match Iceberg Schema)
        arrow_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("amount", pa.decimal128(10, 2), nullable=True),
            pa.field("transaction_date", pa.date32(), nullable=True)
        ])
        
        # 4. Create Data with Schema
        # Note: Use Decimal() for exact fixed-point representation
        data_rows = [
            {"id": 1, "amount": Decimal("100.50"), "transaction_date": date(2023, 1, 1)},
            {"id": 2, "amount": Decimal("250.00"), "transaction_date": date(2023, 1, 2)},
            {"id": 3, "amount": Decimal("50.75"),  "transaction_date": date(2023, 1, 3)},
        ]
        
        arrow_table = pa.Table.from_pylist(data_rows, schema=arrow_schema)
        
        # 5. Append Data
        table.append(arrow_table)
        print("[SUCCESS] 3 records appended to 'default.sales' table.")
        print(f"[VERIFY] Snapshot Summary: {table.current_snapshot().summary}")
        
    except Exception as e:
        print(f"[FAILED] Data ingestion failed: {e}")

if __name__ == "__main__":
    ingest_data()
