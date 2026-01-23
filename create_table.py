from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, DecimalType, DateType

def create_table():
    print("Initializing Iceberg Catalog & Table...")
    
    try:
        # 1. Configure Catalog (SQL-based local catalog)
        # We use a local SQLite file to track the table state (snapshots).
        catalog = load_catalog("default", **{
            "type": "sql",
            "uri": "sqlite:///iceberg_catalog.db",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.region": "us-east-1",
            "warehouse": "s3://warehouse",
        })
        
        # 2. Define Schema
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "amount", DecimalType(10, 2), required=False),
            NestedField(3, "transaction_date", DateType(), required=False),
        )
        
        # 3. Create Table
        table_name = "sales"
        try:
            catalog.create_table(
                identifier=table_name,
                schema=schema,
            )
            print(f"[SUCCESS] Table '{table_name}' created in Catalog.")
        except Exception:
            print(f"[INFO] Table '{table_name}' might already exist.")

    except Exception as e:
        print(f"[FAILED] Could not create table: {e}")

if __name__ == "__main__":
    create_table()
