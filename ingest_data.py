from db_connection import get_duckdb_connection

def ingest_data():
    print("Ingesting sample data into 'sales'...")
    
    try:
        con = get_duckdb_connection()
        
        # 1. Prepare data to insert
        # We use VALUES clause to generate data on the fly
        print("Appending 3 records...")
        query = """
        COPY (
            SELECT * FROM (VALUES 
                (1::BIGINT, 100.50::DECIMAL(10,2), DATE '2023-01-01'),
                (2::BIGINT, 250.00::DECIMAL(10,2), DATE '2023-01-02'),
                (3::BIGINT, 50.75::DECIMAL(10,2),  DATE '2023-01-03')
            ) AS v(id, amount, transaction_date)
        ) 
        TO 's3://warehouse/sales' 
        (FORMAT ICEBERG, MODE 'APPEND');
        """
        
        con.execute(query)
        print("[SUCCESS] 3 records inserted.")
        
        # 2. Verify immediately using read_iceberg
        # Since we are not using a persistent catalog service, we read the path directly
        count = con.execute("SELECT COUNT(*) FROM read_iceberg('s3://warehouse/sales')").fetchone()[0]
        print(f"[VERIFY] Total records in 'sales': {count}")
        
    except Exception as e:
        print(f"[FAILED] Data ingestion failed: {e}")

if __name__ == "__main__":
    ingest_data()
