import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.types import StringType
from datetime import date
from decimal import Decimal

def schema_evolve():
    print("Evolving Schema: Adding 'channel' column...")
    
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
        
        # 2. Add 'channel' Column
        # Check if exists first to make script re-runnable
        has_channel = any(f.name == "channel" for f in table.schema().fields)
        if not has_channel:
            with table.update_schema() as update:
                update.add_column("channel", StringType(), required=False)
            print("[SUCCESS] Column 'channel' added.")
        else:
            print("[INFO] Column 'channel' already exists.")

        # 3. Insert Data into New Schema
        print("Inserting new record with channel info...")
        
        # Define Schema including new column
        arrow_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("amount", pa.decimal128(10, 2), nullable=True),
            pa.field("transaction_date", pa.date32(), nullable=True),
            pa.field("channel", pa.string(), nullable=True) # New Field
        ])
        
        # New record for 'Online' sales
        data = pa.Table.from_pylist([
            {"id": 4, "amount": Decimal("199.99"), "transaction_date": date(2023, 1, 4), "channel": "Online"}
        ], schema=arrow_schema)
        
        table.append(data)
        print("[SUCCESS] New record inserted with channel data.")
        print(f"[VERIFY] Snapshot Summary: {table.current_snapshot().summary}")

    except Exception as e:
        print(f"[FAILED] Schema evolution failed: {e}")

if __name__ == "__main__":
    schema_evolve()
