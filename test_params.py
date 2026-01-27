import requests
import time

API_URL = "http://localhost:8000"

def test_parameter_apis():
    print("--- 🚀 Testing Parameterized Dynamic APIs ---")
    
    table = "dynamic_employees"
    
    # 1. READ SCHEMA
    print(f"\n[1] Getting Schema for {table}...")
    try:
        resp = requests.get(f"{API_URL}/hudi/{table}/schema")
        if resp.status_code == 200:
            print("   ✅ Schema:", resp.json())
        else:
            print(f"   ❌ Schema Failed: {resp.text}")
    except Exception as e:
        print(f"   ❌ Error: {e}")

    # 2. SIMPLE READ (No SQL)
    print(f"\n[2] Reading Data (Columns: full_name, salary)...")
    try:
        resp = requests.get(f"{API_URL}/hudi/{table}/read", params={"columns": "full_name,salary", "limit": 2})
        if resp.status_code == 200:
            data = resp.json()
            print(f"   ✅ Read Success ({len(data)} rows):")
            for row in data:
                print("      ", row)
        else:
            print(f"   ❌ Read Failed: {resp.text}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        
    # 3. FILTERED READ (No SQL)
    print(f"\n[3] Reading Filtered Data (dept=Engineering)...")
    try:
        resp = requests.get(f"{API_URL}/hudi/{table}/read", params={"filter_col": "dept", "filter_val": "Engineering"})
        if resp.status_code == 200:
            data = resp.json()
            print(f"   ✅ Filtered Read Success ({len(data)} rows):")
            for row in data:
                print("      ", row)
        else:
            print(f"   ❌ Filtered Read Failed: {resp.text}")
    except Exception as e:
        print(f"   ❌ Error: {e}")

if __name__ == "__main__":
    test_parameter_apis()
