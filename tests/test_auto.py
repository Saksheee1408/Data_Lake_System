import requests
import io
import time

API_URL = "http://localhost:8000"

def test_auto_detection():
    print("--- 🚀 Testing Auto-Detection APIs ---")

    # 1. Ingest WITHOUT explicit pkey
    # It should detect 'emp_id' because 'id' is in the name or just grab the first column
    table = "auto_detect_table"
    csv_data = """emp_id,name,role
999,Auto Bot,Automation
888,Manual Bot,Labour
"""
    
    print(f"\n[1] Uploading to '{table}' without identifying Primary Key...")
    files = {'file': ('auto.csv', csv_data, 'text/csv')}
    # NO 'pkey' in data!
    data = {'table': table} 
    
    try:
        resp = requests.post(f"{API_URL}/ingest/hudi", files=files, data=data)
        if(resp.status_code == 200):
            print(f"   ✅ Success! {resp.json().get('message')}")
            print(f"   (Check server logs to see if it picked 'emp_id')")
        else:
            print(f"   ❌ Failed: {resp.text}")
    except Exception as e:
        print(f"   ❌ Error: {e}")

    # 2. Verify Data
    print(f"\n[2] Reading '{table}'...")
    time.sleep(2)
    resp = requests.get(f"{API_URL}/hudi/{table}/read")
    if resp.status_code == 200:
        print(f"   ✅ Data Found: {len(resp.json())} rows")
        print("   Row 1:", resp.json()[0])

if __name__ == "__main__":
    test_auto_detection()
