from db_connection import get_duckdb_connection

def read_data():
    print("Reading data from 'default.sales' using DuckDB...")
    
    try:
        con = get_duckdb_connection()
        
        # Path where PyIceberg stored the table. 
        # Layout: warehouse_path / namespace / table_name
        table_path = 's3://warehouse/default/sales'
        
        print(f"Querying Iceberg table at: {table_path}")
        
        # Use iceberg_scan() explicitly
        query = f"SELECT * FROM iceberg_scan('{table_path}')"
        
        # Execute and fetch all
        results = con.execute(query).fetchall()
        
        print(f"\n[SUCCESS] Found {len(results)} records:")
        print("-" * 50)
        print(f"{'ID':<5} | {'Amount':<15} | {'Date':<12}")
        print("-" * 50)
        
        for row in results:
            print(f"{row[0]:<5} | {row[1]:<15} | {row[2]}")
            
    except Exception as e:
        print(f"[FAILED] Could not read data: {e}")
        # Debug: list files to see if the path is correct
        try:
            print("\ndebug: Checking files in explicit path...")
            files = con.execute(f"SELECT * FROM glob('{table_path}/**')").fetchall()
            if files:
                print(f"Found {len(files)} files/objects in path.")
            else:
                print("Path appears empty or inaccessible.")
        except:
            pass

if __name__ == "__main__":
    read_data()
