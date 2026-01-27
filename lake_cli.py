import argparse
import requests
import os
import json
import sys

API_URL = "http://localhost:8000"

def ingest(args):
    if not os.path.exists(args.file):
        print(f"❌ Error: File '{args.file}' not found.")
        return

    print(f"🚀 Ingesting '{args.file}' into table '{args.table}'...")
    
    files = {'file': open(args.file, 'rb')}
    data = {'table': args.table}
    
    if args.pkey:
        data['pkey'] = args.pkey
    
    try:
        resp = requests.post(f"{API_URL}/ingest/hudi", files=files, data=data)
        if resp.status_code == 200:
            print("✅ Success!")
            print(json.dumps(resp.json(), indent=2))
        else:
            print(f"❌ Failed: {resp.text}")
    except Exception as e:
        print(f"❌ Error: {e}")

def read(args):
    print(f"🔍 Reading from table '{args.table}'...")
    
    params = {}
    if args.columns:
        params['columns'] = args.columns
    if args.limit:
        params['limit'] = args.limit
        
    try:
        resp = requests.get(f"{API_URL}/hudi/{args.table}/read", params=params)
        if resp.status_code == 200:
            data = resp.json()
            print(f"✅ Found {len(data)} rows:")
            print(json.dumps(data, indent=2))
        else:
            print(f"❌ Failed: {resp.text}")
    except Exception as e:
        print(f"❌ Error: {e}")

def delete(args):
    print(f"🗑 Deleting IDs [{args.ids}] from '{args.table}'...")
    
    data = {
        'table': args.table,
        'ids': args.ids,
        'pkey': args.pkey 
    }
    
    try:
        resp = requests.post(f"{API_URL}/delete/hudi", data=data)
        if resp.status_code == 200:
            print("✅ Success!")
            print(json.dumps(resp.json(), indent=2))
        else:
            print(f"❌ Failed: {resp.text}")
    except Exception as e:
        print(f"❌ Error: {e}")

def main():
    parser = argparse.ArgumentParser(description="Data Lake CLI Tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # INGEST Command
    p_ingest = subparsers.add_parser("ingest", help="Upload CSV to Data Lake")
    p_ingest.add_argument("file", help="Path to CSV file")
    p_ingest.add_argument("table", help="Target Table Name")
    p_ingest.add_argument("--pkey", help="Primary Key (Optional, auto-detected if omitted)")

    # READ Command
    p_read = subparsers.add_parser("read", help="Read data from Data Lake")
    p_read.add_argument("table", help="Table Name")
    p_read.add_argument("--columns", help="Columns to select (comma-separated)", default="*")
    p_read.add_argument("--limit", help="Max rows to return", default=100)

    # DELETE Command
    p_delete = subparsers.add_parser("delete", help="Delete records")
    p_delete.add_argument("table", help="Table Name")
    p_delete.add_argument("ids", help="Comma-separated IDs to delete")
    p_delete.add_argument("pkey", help="Primary Key column name")

    args = parser.parse_args()

    if args.command == "ingest":
        ingest(args)
    elif args.command == "read":
        read(args)
    elif args.command == "delete":
        delete(args)

if __name__ == "__main__":
    main()
