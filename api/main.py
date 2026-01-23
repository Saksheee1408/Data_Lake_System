from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from datetime import date
from decimal import Decimal
import duckdb
import io
import sys
import os

# Add parent dir to path so we can import db_connection if needed, 
# but for now we'll inline the connection logic to keep API self-contained and robust.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from db_connection import get_duckdb_connection
except ImportError:
    # Fallback if running directly from api folder without setting pythonpath
    def get_duckdb_connection():
         con = duckdb.connect(database=':memory:')
         con.execute("INSTALL httpfs; LOAD httpfs;")
         con.execute("INSTALL iceberg; LOAD httpfs;") # Try install, might fail if loaded
         con.execute("SET s3_endpoint='localhost:9000';")
         con.execute("SET s3_access_key_id='minioadmin';")
         con.execute("SET s3_secret_access_key='minioadmin';")
         con.execute("SET s3_use_ssl=false;")
         con.execute("SET s3_region='us-east-1';")
         con.execute("SET s3_url_style='path';")
         return con

app = FastAPI(title="Data Lakehouse API")

# Allow CORS for Angular
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_catalog():
    # Calculate absolute path to catalog in project root (one level up)
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    catalog_db_path = os.path.join(root_dir, "iceberg_catalog.db")
    
    print(f"DEBUG: Root Dir: {root_dir}")
    print(f"DEBUG: Catalog Path: {catalog_db_path}")
    print(f"DEBUG: Catalog DB Exists? {os.path.exists(catalog_db_path)}")
    print(f"DEBUG: URI: sqlite:///{catalog_db_path}")

    return load_catalog("default", **{
        "type": "sql",
        "uri": f"sqlite:///{catalog_db_path}",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.region": "us-east-1",
        "warehouse": "s3://warehouse",
    })

@app.get("/")
def health_check():
    return {"status": "ok", "message": "Lakehouse API is running"}

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    print(f"Receiving file: {file.filename}")
    try:
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        
        # 1. Prepare Schema & Data Conversion
        # We need strict types for Iceberg
        arrow_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("amount", pa.decimal128(10, 2), nullable=True),
            pa.field("transaction_date", pa.date32(), nullable=True),
            pa.field("channel", pa.string(), nullable=True)
        ])
        
        # Convert DataFrame to list of dicts for safe PyArrow conversion
        data_rows = []
        for _, row in df.iterrows():
            item = {
                "id": int(row['id']),
                "amount": Decimal(str(row['amount'])) if pd.notnull(row['amount']) else None,
                "transaction_date": date.fromisoformat(str(row['transaction_date'])) if pd.notnull(row['transaction_date']) else None,
                "channel": str(row['channel']) if 'channel' in row and pd.notnull(row['channel']) else None
            }
            data_rows.append(item)
            
        arrow_table = pa.Table.from_pylist(data_rows, schema=arrow_schema)
        
        # 2. Ingest to Iceberg
        catalog = get_catalog()
        table = catalog.load_table("default.sales")
        table.append(arrow_table)
        
        return {"status": "success", "message": f"Ingested {len(data_rows)} rows"}
        
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query")
def run_query(sql: str = "SELECT * FROM sales"):
    try:
        con = get_duckdb_connection()
        
        # Helper: Replace simple table name with fully qualified path if needed (simple hack for UX)
        # If user says "SELECT * FROM sales", we map it to our iceberg table path logic
        # Ideally, we should just let them write valid SQL, but let's be helpful.
        
        catalog = get_catalog()
        try:
            table = catalog.load_table("default.sales")
            # For this demo, we assume 'sales' in SQL refers to the iceberg table
            # We can register it as a view
            meta_loc = table.metadata_location
            con.execute(f"CREATE OR REPLACE VIEW sales AS SELECT * FROM iceberg_scan('{meta_loc}')")
        except:
            pass # Maybe table doesn't exist yet
            
        # Run Query
        results = con.execute(sql).fetchdf()
        
        # Convert to JSON-friendly format (Handling dates/NaNs)
        return results.to_dict(orient="records")
        
    except Exception as e:
         raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
