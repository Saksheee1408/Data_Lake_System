import requests
import time
import json

API_URL = "http://localhost:8000"

def test_iceberg():
    print("--- ❄️ Testing Iceberg Ingestion & Time Travel Support ❄️ ---")
    
    table_name = "iceberg_sales"
    
    # 1. Ingest Data (Snapshot 1)
    csv_1 = """id,item,price,ts
1,Laptop,1000,2024-01-01
2,Mouse,20,2024-01-01
"""
    print(f"\n[1] Ingesting Snapshot 1 into '{table_name}'...")
    files = {'file': ('batch1.csv', csv_1, 'text/csv')}
    # Using the generic ingest endpoint which maps to Iceberg in api/main.py
    resp = requests.post(f"{API_URL}/ingest/{table_name}", files=files)
    if resp.status_code == 200:
        print("   ✅ Success:", resp.json()['message'])
    else:
        print("   ❌ Failed:", resp.text)
        return

    time.sleep(2)

    # 2. Ingest More Data (Snapshot 2)
    csv_2 = """id,item,price,ts
3,Keyboard,50,2024-01-02
"""
    print(f"\n[2] Ingesting Snapshot 2 (Append)...")
    files = {'file': ('batch2.csv', csv_2, 'text/csv')}
    resp = requests.post(f"{API_URL}/ingest/{table_name}", files=files)
    if resp.status_code == 200:
        print("   ✅ Success:", resp.json()['message'])
    else:
        print("   ❌ Failed:", resp.text)

    # 3. Query Current State
    print(f"\n[3] Querying Current State...")
    # Use Trino explicitly for Iceberg
    resp = requests.get(f"{API_URL}/query/trino", params={"sql": f"SELECT * FROM {table_name}", "catalog": "iceberg"})
    if resp.status_code == 200:
        print(f"   ✅ Rows: {len(resp.json())}")
        print(json.dumps(resp.json(), indent=2))
    else:
        print("   ❌ Query Failed:", resp.text)

if __name__ == "__main__":
    test_iceberg()
