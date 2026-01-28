import requests
import pandas as pd
import io
import os

# Configuration
API_URL = "http://localhost:8000"

def test_workflow():
    print(f"Testing API at: {API_URL}")
    
    # 1. Create a dummy CSV in memory
    print("\n[1] Generating Test Data...")
    csv_content = """id,amount,transaction_date,channel
101,50.00,2023-01-10,API_Test
102,75.50,2023-01-11,API_Test
"""
    # Create a file-like object
    files = {'file': ('test_batch.csv', csv_content, 'text/csv')}

    # 2. Upload Data
    print("[2] Uploading CSV...")
    try:
        response = requests.post(f"{API_URL}/upload", files=files)
        if response.status_code == 200:
            print(f"✅ Upload Success: {response.json()}")
        else:
            print(f"❌ Upload Failed: {response.text}")
            return
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    # 3. Query Data
    print("\n[3] Verifying Data (Querying)...")
    try:
        # Query for the records we just added
        sql = "SELECT * FROM sales WHERE channel = 'API_Test'"
        response = requests.get(f"{API_URL}/query", params={"sql": sql})
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Query Success. Found {len(data)} rows:")
            for row in data:
                print(row)
            
            if len(data) == 2:
                print("\n🎉 TEST PASSED: All records found!")
            else:
                print("\n⚠️ TEST WARNING: Expected 2 rows.")
        else:
            print(f"❌ Query Failed: {response.text}")

    except Exception as e:
        print(f"❌ Connection Failed: {e}")

if __name__ == "__main__":
    # Ensure the API is running before executing
    test_workflow()
