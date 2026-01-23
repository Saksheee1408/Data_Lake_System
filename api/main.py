from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io.pyarrow import pyarrow_to_schema
import duckdb
import io
import sys
import os

# Add parent dir to path so we can import db_connection if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

app = FastAPI(title="Data Lakehouse API")

# Allow CORS
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
    
    print(f"DEBUG: Catalog Path: {catalog_db_path}")

    return load_catalog("default", **{
        "type": "sql",
        "uri": f"sqlite:///{catalog_db_path}",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.region": "us-east-1",
        "warehouse": "s3://warehouse",
    })

def get_duckdb_connection():
    # Fresh connection for every request to ensure clean state
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD httpfs;") 
    con.execute("SET s3_endpoint='localhost:9000';")
    con.execute("SET s3_access_key_id='minioadmin';")
    con.execute("SET s3_secret_access_key='minioadmin';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_region='us-east-1';")
    con.execute("SET s3_url_style='path';")
    return con

@app.get("/")
def health_check():
    return {"status": "ok", "message": "Lakehouse API is running"}

@app.post("/ingest/{table_name}")
async def ingest_table_dynamic(table_name: str, file: UploadFile = File(...)):
    print(f"Receiving file for table: {table_name}")
    try:
        contents = await file.read()
        # Read CSV to Pandas (auto-detect types)
        df = pd.read_csv(io.BytesIO(contents))
        
        # Convert to PyArrow Table (Infer Schema)
        pa_table = pa.Table.from_pandas(df)
        
        # Get Catalog
        catalog = get_catalog()
        full_table_name = f"default.{table_name}"
        
        try:
            # Try loading the table
            table = catalog.load_table(full_table_name)
            print(f"Table '{full_table_name}' exists. Appending data...")
            
            # TODO: In a real prod system, you might want to evolve schema here if new cols exist
            # For now, we assume schema is compatible or PyIceberg will throw error
            table.append(pa_table)
            action = "appended"
            
        except NoSuchTableError:
            # Create Table
            print(f"Table '{full_table_name}' does not exist. Creating...")
            
            # Convert Arrow Schema to Iceberg Schema
            iceberg_schema = pyarrow_to_schema(pa_table.schema)
            
            table = catalog.create_table(
                identifier=full_table_name,
                schema=iceberg_schema
            )
            table.append(pa_table)
            action = "created"
            
        return {
            "status": "success", 
            "message": f"Successfully {action} data to {full_table_name}",
            "rows": len(df),
            "schema": str(pa_table.schema)
        }
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query")
def run_query(sql: str = "SELECT * FROM sales"):
    try:
        con = get_duckdb_connection()
        catalog = get_catalog()
        
        # Auto-register ALL tables in 'default' namespace as Views
        # This allows queries like "SELECT * FROM products" to work dynamically
        try:
            tables = catalog.list_tables("default")
            print(f"Found tables: {tables}")
            
            for tbl in tables:
                # tbl is usually a tuple ('default', 'sales') or just identifier class
                # PyIceberg list_tables returns list of Identifiers
                # Let's handle tuple or object
                if isinstance(tbl, tuple):
                     tbl_name = tbl[-1] # valid for ('default', 'sales')
                else:
                     tbl_name = tbl.name 
                     
                fullname = f"default.{tbl_name}"
                params = catalog.load_table(fullname)
                loc = params.metadata_location
                
                # Register View
                # Note: DuckDB queries are case insensitive usually, but let's be safe
                print(f"Registering view '{tbl_name}' -> {loc}")
                con.execute(f"CREATE OR REPLACE VIEW {tbl_name} AS SELECT * FROM iceberg_scan('{loc}')")
                
        except Exception as e:
            print(f"Warning during view registration: {e}")
            
        # Run Query
        print(f"Executing SQL: {sql}")
        results = con.execute(sql).fetchdf()
        
        # Handle NaN/Inf for JSON serialization
        results = results.where(pd.notnull(results), None)
        
        return results.to_dict(orient="records")
        
    except Exception as e:
         print(f"Query Error: {e}")
         raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
