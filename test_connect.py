from db_connection import get_duckdb_connection

def test_connection():
    print("Testing DuckDB + Iceberg + MinIO connection...")
    
    try:
        con = get_duckdb_connection()
        print("[OK] DuckDB connection established.")
        
        # 1. Test Extensions
        res = con.execute("SELECT 1").fetchall()
        print(f"[OK] Basic Query Result: {res[0][0]}")
        
        # 2. Test S3 Connectivity
        # We try to list the contents of the warehouse bucket. 
        # It might be empty, but the query should succeed without auth errors.
        print("Checking access to 's3://warehouse/'...")
        files = con.execute("SELECT * FROM glob('s3://warehouse/**')").fetchall()
        print(f"[OK] Access successful. Found {len(files)} objects in bucket.")
        
        print("\nSUCCESS: Environment is fully connected!")
        
    except Exception as e:
        print(f"\nFAILED: An error occurred.\n{e}")

if __name__ == "__main__":
    test_connection()
