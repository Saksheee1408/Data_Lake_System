import requests
import os
import sys

# Configuration
API_URL = "http://localhost:8000"

def test_hudi_ingest():
    print(f"Testing Hudi Ingestion API at: {API_URL}")
    
    # Create a small test CSV
    csv_content = """Index,Name,Date,Amount
1,Alice,2023-01-01,100
2,Bob,2023-01-02,200
"""
    files = {'file': ('test_hudi.csv', csv_content, 'text/csv')}
    data = {
        'table': 'leads_hudi_test_api',
        'pkey': 'Index',
        'partition': '',
        'precombine': 'Date'
    }

    print("\n[1] Uploading CSV to /ingest/hudi...")
    try:
        response = requests.post(f"{API_URL}/ingest/hudi", files=files, data=data)
        if response.status_code == 200:
            print(f"✅ Ingestion Success: {response.json()}")
        else:
            print(f"❌ Ingestion Failed: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"❌ Connection Failed: {e}")

if __name__ == "__main__":
    test_hudi_ingest()
