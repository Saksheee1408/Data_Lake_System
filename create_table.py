from db_connection import get_duckdb_connection

def create_table():
    print("Initializing Iceberg Table 'sales'...")
    
    try:
        con = get_duckdb_connection()
        
        # 1. Define a local DuckDB table to establish the schema
        con.execute("""
            CREATE OR REPLACE TABLE local_schema_def (
                id BIGINT,
                amount DECIMAL(10, 2),
                transaction_date DATE
            )
        """)
        
        # 2. Initialize the Iceberg table in MinIO using COPY
        # We copy 0 rows to create the metadata/schema without data.
        # ALLOW_OVERWRITE=true allows this script to reset the table if run again.
        print("Creating Iceberg metadata at s3://warehouse/sales...")
        con.execute("""
            COPY (SELECT * FROM local_schema_def LIMIT 0) 
            TO 's3://warehouse/sales' 
            (FORMAT ICEBERG, ALLOW_OVERWRITE true)
        """)
        
        print("[SUCCESS] Iceberg Table 'sales' initialized at s3://warehouse/sales")
        
    except Exception as e:
        print(f"[FAILED] Could not create table: {e}")

if __name__ == "__main__":
    create_table()
