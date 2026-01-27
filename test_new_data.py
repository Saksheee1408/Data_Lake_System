import requests
import time
import random

API_URL = "http://localhost:8000"

def run_demo():
    print("--- 🌟 Testing Complete Lifecycle on NEW Data (Sales) ---")
    
    # ---------------------------------------------------------
    # 1. PREPARE DATA
    # ---------------------------------------------------------
    table = "sales_2024"
    print(f"\n[1] Defining table: '{table}'")
    
    # Notice: 'transaction_id' will be auto-detected as Primary Key because it contains 'id'
    csv_content = """transaction_id,customer,product,amount,region
T-1001,Alice,Laptop,1200,US
T-1002,Bob,Mouse,25,EU
T-1003,Charlie,Monitor,300,US
T-1004,David,Keyboard,50,Asia
"""
    print(f"   -> Data Preview:\n{csv_content}")

    # ---------------------------------------------------------
    # 2. INGEST (Create)
    # ---------------------------------------------------------
    print("[2] Ingesting Data (Auto-detecting schema & keys)...")
    files = {'file': ('sales.csv', csv_content, 'text/csv')}
    # We ONLY send table name. API finds 'transaction_id' automatically.
    resp = requests.post(f"{API_URL}/ingest/hudi", files=files, data={'table': table})
    
    if resp.status_code == 200:
        print(f"   ✅ Success: {resp.json().get('message')}")
    else:
        print(f"   ❌ Failed: {resp.text}")
        return

    time.sleep(5) # Allow Spark to finish writing

    # ---------------------------------------------------------
    # 3. READ
    # ---------------------------------------------------------
    print("\n[3] Reading Data...")
    resp = requests.get(f"{API_URL}/hudi/{table}/read")
    data = resp.json()
    print(f"   ✅ Found {len(data)} records.")
    for row in data:
        print(f"      - {row['customer']} bought {row['product']} for ${row['amount']}")

    # ---------------------------------------------------------
    # 4. UPDATE (Upsert)
    # ---------------------------------------------------------
    print("\n[4] Updating: Bob (T-1002) returns Mouse, buys iMac ($2000)...")
    
    # To update, we just send the row again with the SAME ID (T-1002)
    update_csv = """transaction_id,customer,product,amount,region
T-1002,Bob,iMac 27,2000,EU
"""
    files = {'file': ('update.csv', update_csv, 'text/csv')}
    # We must match the same parameters (implicit auto-detect works same way)
    requests.post(f"{API_URL}/ingest/hudi", files=files, data={'table': table})
    
    print("   -> Update sent. Verifying...")
    time.sleep(5)
    
    # Check Bob's new Amount
    data = requests.get(f"{API_URL}/hudi/{table}/read", params={"filter_col": "transaction_id", "filter_val": "T-1002"}).json()
    if data and data[0]['amount'] == 2000.0:
        print("   ✅ Verified: Bob spent $2000 (Update Successful)")
    else:
        print(f"   ❌ Update check failed. Data: {data}")

    # ---------------------------------------------------------
    # 5. DELETE
    # ---------------------------------------------------------
    print("\n[5] Deleting: David (T-1004)...")
    # For delete, we explicitly say what ID to kill
    requests.post(f"{API_URL}/delete/hudi", data={
        'table': table,
        'pkey': 'transaction_id', # We specify pkey for delete to be safe
        'ids': 'T-1004'
    })
    
    time.sleep(5)
    
    # Verify Count (Should be 3 now: Alice, Bob, Charlie)
    data = requests.get(f"{API_URL}/hudi/{table}/read").json()
    if len(data) == 3:
         print("   ✅ Verified: 3 records remain (Delete Successful)")
    else:
         print(f"   ❌ Delete check failed. Count: {len(data)}")

    print("\n--- 🎉 Demo Complete ---")

if __name__ == "__main__":
    run_demo()
