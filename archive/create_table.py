from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, DecimalType, DateType
from pyiceberg.exceptions import NamespaceAlreadyExistsError

def create_table():
    print("Initializing Iceberg Catalog & Table...")
    
    try:
        # 1. Configure Catalog
        catalog = load_catalog("default", **{
            "type": "sql",
            "uri": "sqlite:///iceberg_catalog.db",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.region": "us-east-1",
            "warehouse": "s3://warehouse",
        })
        
        # 2. Ensure Schema/Namespace 'default' exists
        try:
            catalog.create_namespace("default")
            print("[SUCCESS] Namespace 'default' created.")
        except NamespaceAlreadyExistsError:
            print("[INFO] Namespace 'default' already exists.")
        except Exception as e:
            # Some catalogs might raise other errors or not support this
            print(f"[WARN] Could not create namespace: {e}")

        # 3. Define Schema and Create Table
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "amount", DecimalType(10, 2), required=False),
            NestedField(3, "transaction_date", DateType(), required=False),
        )
        
        table_name = "default.sales"
        try:
            catalog.create_table(
                identifier=table_name,
                schema=schema,
            )
            print(f"[SUCCESS] Table '{table_name}' created.")
        except Exception as e:
            # Check if it really failed or just exists
            print(f"[INFO] Table creation message: {e}")

    except Exception as e:
        print(f"[FAILED] Critical error in create_table: {e}")

if __name__ == "__main__":
    create_table()
