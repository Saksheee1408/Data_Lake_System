import requests
import time

API_URL = "http://localhost:8000"
TABLE_NAME = "hudi_crud_test"

def test_crud_lifecycle():
    print(f"--- Starting CRUD Test for Table: {TABLE_NAME} ---")
    
    # ---------------------------------------------------------
    # 1. CREATE (Ingest Initial Data)
    # ---------------------------------------------------------
    print("\n[1] CREATE: Ingesting 2 records...")
    csv_1 = """id,name,role,ts
1,Alice,Engineer,2023-01-01
2,Bob,Manager,2023-01-01
"""
    files = {'file': ('batch1.csv', csv_1, 'text/csv')}
    data = {'table': TABLE_NAME, 'pkey': 'id', 'precombine': 'ts'}
    
    resp = requests.post(f"{API_URL}/ingest/hudi", files=files, data=data)
    if resp.status_code == 200:
        print("✅ Create Success")
    else:
        print(f"❌ Create Failed: {resp.text}")
        return

    # Wait a bit for Trino to sync (metastore sync happens in script, but Trino might cache)
    # Usually instant if we query directly.
    time.sleep(2)

    # ---------------------------------------------------------
    # 2. READ (Verify Create)
    # ---------------------------------------------------------
    print("\n[2] READ: Verifying Initial Data...")
    sql = f"SELECT * FROM {TABLE_NAME} ORDER BY id"
    try:
        resp = requests.get(f"{API_URL}/query/hudi", params={"sql": sql})
        if resp.status_code == 200:
            rows = resp.json()
            print(f"Found {len(rows)} rows: {rows}")
            if len(rows) == 2 and rows[0]['name'] == 'Alice':
                print("✅ Read Verification Success")
            else:
                print("❌ Read Verification Failed (Data mismatch)")
        else:
            print(f"❌ Read Failed: {resp.text}")
    except Exception as e:
        print(f"❌ Read Error: {e}")

    # ---------------------------------------------------------
    # 3. UPDATE (Upsert - Change Bob's Role)
    # ---------------------------------------------------------
    print("\n[3] UPDATE: Changing Bob's role to 'Director'...")
    csv_2 = """id,name,role,ts
2,Bob,Director,2023-01-02
"""
    files = {'file': ('batch2.csv', csv_2, 'text/csv')}
    resp = requests.post(f"{API_URL}/ingest/hudi", files=files, data=data)
    if resp.status_code == 200:
        print("✅ Update Success")
    else:
        print(f"❌ Update Failed: {resp.text}")
        return

    time.sleep(2)

    # ---------------------------------------------------------
    # 4. READ (Verify Update)
    # ---------------------------------------------------------
    print("\n[4] READ: Verifying Update...")
    try:
        resp = requests.get(f"{API_URL}/query/hudi", params={"sql": sql})
        rows = resp.json()
        bob = next((r for r in rows if r['id'] == 2), None)
        if bob and bob['role'] == 'Director':
             print(f"✅ Update Verified: Bob is now {bob['role']}")
        else:
             print(f"❌ Update Verification Failed. Bob: {bob}")
    except Exception as e:
        print(f"❌ Read Error: {e}")

    # ---------------------------------------------------------
    # 5. DELETE (Delete Alice)
    # ---------------------------------------------------------
    print("\n[5] DELETE: Removing Alice (id=1)...")
    delete_data = {
        'table': TABLE_NAME,
        'pkey': 'id',
        'ids': '1'
    }
    resp = requests.post(f"{API_URL}/delete/hudi", data=delete_data)
    if resp.status_code == 200:
        print("✅ Delete Success")
        print(resp.json()['message'])
    else:
        print(f"❌ Delete Failed: {resp.text}")
        return

    time.sleep(2)

    # ---------------------------------------------------------
    # 6. READ (Verify Delete)
    # ---------------------------------------------------------
    print("\n[6] READ: Verifying Delete...")
    try:
        resp = requests.get(f"{API_URL}/query/hudi", params={"sql": sql})
        rows = resp.json()
        print(f"Found {len(rows)} rows: {rows}")
        if len(rows) == 1 and rows[0]['id'] == 2:
            print("✅ Delete Verified: Only Bob remains.")
        else:
            print("❌ Delete Verification Failed")
    except Exception as e:
        print(f"❌ Read Error: {e}")

if __name__ == "__main__":
    test_crud_lifecycle()
