import requests
import io
import pandas as pd
import time

API_URL = "http://localhost:8000"

def test_dynamic_scenarios():
    print("--- 🚀 Starting Dynamic Data Lake Test ---")

    # ------------------------------------------------------------
    # SCENARIO 1: EMPLOYEES (Standard Data)
    # ------------------------------------------------------------
    table_emp = "dynamic_employees"
    print(f"\n[1] Testing Table: {table_emp}")
    csv_emp = """emp_id,full_name,dept,salary,join_date
101,John Doe,Engineering,85000,2023-01-15
102,Jane Smith,Marketing,78000,2023-02-01
103,Sam Brown,Sales,65000,2023-03-10
"""
    print("   -> Uploading Employees CSV...")
    files = {'file': ('employees.csv', csv_emp, 'text/csv')}
    data = {'table': table_emp, 'pkey': 'emp_id', 'partition': 'dept', 'precombine': 'join_date'}
    
    resp = requests.post(f"{API_URL}/ingest/hudi", files=files, data=data)
    if resp.status_code == 200:
        print(f"   ✅ Ingestion Success: {resp.json()['message']}")
    else:
        print(f"   ❌ Ingestion Failed: {resp.text}")

    # ------------------------------------------------------------
    # SCENARIO 2: PRODUCTS (Different Schema)
    # ------------------------------------------------------------
    table_prod = "dynamic_products"
    print(f"\n[2] Testing Table: {table_prod}")
    csv_prod = """sku,product_name,category,price,stock_qty
P-001,Gaming Laptop,Electronics,1200.50,15
P-002,Wireless Mouse,Electronics,25.99,100
P-003,Coffee Mug,Home,12.00,50
"""
    print("   -> Uploading Products CSV...")
    files = {'file': ('products.csv', csv_prod, 'text/csv')}
    data = {'table': table_prod, 'pkey': 'sku', 'partition': 'category'}
    
    resp = requests.post(f"{API_URL}/ingest/hudi", files=files, data=data)
    if resp.status_code == 200:
        print(f"   ✅ Ingestion Success: {resp.json()['message']}")
    else:
        print(f"   ❌ Ingestion Failed: {resp.text}")

    # ------------------------------------------------------------
    # SCENARIO 3: SCHEMA EVOLUTION (Adding a column to Employees)
    # ------------------------------------------------------------
    print(f"\n[3] Testing Schema Evolution (Adding 'bonus' to {table_emp})")
    csv_emp_v2 = """emp_id,full_name,dept,salary,join_date,bonus
101,John Doe,Engineering,90000,2023-01-15,5000
104,Alice Green,HR,72000,2023-04-20,2000
"""
    print("   -> Uploading Employees Upsert with New Column...")
    files = {'file': ('employees_v2.csv', csv_emp_v2, 'text/csv')}
    data = {'table': table_emp, 'pkey': 'emp_id', 'partition': 'dept', 'precombine': 'join_date'}
    
    resp = requests.post(f"{API_URL}/ingest/hudi", files=files, data=data)
    if resp.status_code == 200:
        print(f"   ✅ Evolution Ingestion Success: {resp.json()['message']}")
    else:
        print(f"   ❌ Evolution Failed: {resp.text}")

    # ------------------------------------------------------------
    # VERIFICATION
    # ------------------------------------------------------------
    print("\n[4] Verifying Data via Trino...")
    time.sleep(2) # Give a moment for metadata sync
    
    print(f"   -> Querying {table_emp} (should have bonus column)...")
    resp = requests.get(f"{API_URL}/query/hudi", params={"sql": f"SELECT emp_id, full_name, salary, bonus FROM {table_emp} ORDER BY emp_id"})
    if resp.status_code == 200:
        data = resp.json()
        print(f"   ✅ Result: {data}")
    else:
        print(f"   ❌ Query Failed: {resp.text}")

    print(f"\n   -> Querying {table_prod}...")
    resp = requests.get(f"{API_URL}/query/hudi", params={"sql": f"SELECT sku, product_name, price FROM {table_prod} ORDER BY sku"})
    if resp.status_code == 200:
        data = resp.json()
        print(f"   ✅ Result: {data}")

if __name__ == "__main__":
    test_dynamic_scenarios()
