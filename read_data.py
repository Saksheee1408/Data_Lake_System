from db_connection import get_duckdb_connection

def read_data():
    print("Reading data from 'default.sales' using DuckDB...")
    
    try:
        con = get_duckdb_connection()
        
        # Path where PyIceberg stored the table. 
        # Since we used namespace 'default' and table 'sales', and warehouse 's3://warehouse',
        # standard layout is usually s3://warehouse/default/sales
        table_path = 's3://warehouse/default/sales'
        
        print(f"Querying Iceberg table at: {table_path}")
        
        query = f"SELECT * FROM read_iceberg('{table_path}')"
        
        # Execute and fetch all
        results = con.execute(query).fetchall()
        
        print(f"\n[SUCCESS] Found {len(results)} records:")
        print("-" * 40)
        print(f"{'ID':<5} | {'Amount':<10} | {'Date':<12}")
        print("-" * 40)
        
        for row in results:
            print(f"{row[0]:<5} | {row[1]:<10} | {row[2]}")
            
    except Exception as e:
        print(f"[FAILED] Could not read data: {e}")
        print("Tip: Check if the path 's3://warehouse/default/sales' exists in MinIO.")

if __name__ == "__main__":
    read_data()
