import requests
import pandas as pd
import io

# Configuration
API_URL = "http://localhost:8000"

def test_dynamic_workflow():
    print(f"Testing Dynamic API at: {API_URL}")
    
    # 1. Create a NEW CSV (New Schema: Products)
    print("\n[1] Generating Products Data...")
    csv_content = """product_id,product_name,category,price,in_stock
P001,Gaming Laptop,Electronics,1200.50,true
P002,Wireless Mouse,Electronics,25.99,true
P003,Coffee Maker,Home,89.00,false
"""
    files = {'file': ('products.csv', csv_content, 'text/csv')}

    # 2. Upload Data to 'products' table
    # This should AUTO-CREATE the 'default.products' table
    print("[2] Ingesting to 'products' table...")
    try:
        response = requests.post(f"{API_URL}/ingest/products", files=files)
        if response.status_code == 200:
            print(f"✅ Ingest Success: {response.json()['message']}")
        else:
            print(f"❌ Ingest Failed: {response.text}")
            return
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    # 3. Query 'products' table
    print("\n[3] Verifying Data (Querying 'products')...")
    try:
        sql = "SELECT * FROM products WHERE price > 50 ORDER BY price DESC"
        response = requests.get(f"{API_URL}/query", params={"sql": sql})
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Query Success. Found {len(data)} rows:")
            for row in data:
                print(row)
            
            # Validation
            if len(data) == 2: # Laptop and Coffee Maker
                print("\n🎉 TEST PASSED: Dynamic Table Creation & Querying works!")
            else:
                print(f"\n⚠️ TEST WARNING: Expected 2 rows, got {len(data)}.")
        else:
            print(f"❌ Query Failed: {response.text}")

    except Exception as e:
        print(f"❌ Connection Failed: {e}")

if __name__ == "__main__":
    test_dynamic_workflow()
