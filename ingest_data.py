from db_connection import get_duckdb_connection
from datetime import date

def ingest_data():
    print("Ingesting sample data into 'sales'...")
    
    try:
        con = get_duckdb_connection()
        
        # Insert a few records
        # DuckDB's Iceberg support handles the commit and metadata update
        query = """
        INSERT INTO sales VALUES 
        (1, 100.50, DATE '2023-01-01'),
        (2, 250.00, DATE '2023-01-02'),
        (3, 50.75,  DATE '2023-01-03');
        """
        
        con.execute(query)
        print("[SUCCESS] 3 records inserted.")
        
        # Verify immediately
        count = con.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
        print(f"[VERIFY] Total records in 'sales': {count}")
        
    except Exception as e:
        print(f"[FAILED] Data ingestion failed: {e}")

if __name__ == "__main__":
    ingest_data()
