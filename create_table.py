from db_connection import get_duckdb_connection

def create_table():
    print("Initializing Iceberg Table 'sales'...")
    
    try:
        con = get_duckdb_connection()
        
        # Define the basic Sales schema
        # We use standard SQL. DuckDB handles the Iceberg format details.
        # Location specifies where in MinIO the table lives.
        query = """
        CREATE TABLE IF NOT EXISTS sales (
            id BIGINT,
            amount DECIMAL(10, 2),
            transaction_date DATE
        ) 
        FORMAT 'ICEBERG' 
        EXTERNAL_LOCATION 's3://warehouse/sales';
        """
        
        con.execute(query)
        print("[SUCCESS] Table 'sales' created (or already exists) at s3://warehouse/sales")
        
    except Exception as e:
        print(f"[FAILED] Could not create table: {e}")

if __name__ == "__main__":
    create_table()
