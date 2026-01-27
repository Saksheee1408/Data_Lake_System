from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
import subprocess
import shutil
import uuid
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, LongType, DoubleType, BooleanType, 
    TimestampType, DateType, DecimalType, FloatType, IntegerType
)
import duckdb
import io
import sys
import os
import trino

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
        "s3.endpoint": "http://localhost:9002",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.region": "us-east-1",
        "warehouse": "s3://warehouse",
    })

def get_duckdb_connection():
    # Fresh connection for every request to ensure clean state
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;") 
    con.execute("SET s3_endpoint='localhost:9002';")
    con.execute("SET s3_access_key_id='minioadmin';")
    con.execute("SET s3_secret_access_key='minioadmin';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_region='us-east-1';")
    con.execute("SET s3_url_style='path';")
    return con

def map_arrow_type_to_iceberg(pa_type):
    """
    Simple mapper from PyArrow types to Iceberg types.
    Expanded as needed for more complex types.
    """
    if pa.types.is_int64(pa_type):
        return LongType()
    elif pa.types.is_int32(pa_type):
        return IntegerType()
    elif pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
        return StringType()
    elif pa.types.is_float64(pa_type):
        return DoubleType()
    elif pa.types.is_float32(pa_type):
        return FloatType()
    elif pa.types.is_boolean(pa_type):
        return BooleanType()
    elif pa.types.is_timestamp(pa_type):
        return TimestampType()
    elif pa.types.is_date32(pa_type) or pa.types.is_date64(pa_type):
        return DateType()
    # Fallback to string for unknown types to be safe
    return StringType()

def infer_iceberg_schema(pa_schema) -> Schema:
    """
    Manually constructs an Iceberg Schema from a PyArrow Schema.
    Assigns field IDs sequentially starting from 1.
    """
    fields = []
    for i, field in enumerate(pa_schema, start=1):
        iceberg_type = map_arrow_type_to_iceberg(field.type)
        fields.append(
            NestedField(
                field_id=i, 
                name=field.name, 
                field_type=iceberg_type, 
                required=not field.nullable
            )
        )
    return Schema(*fields)

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
            
            # TODO: Handle schema evolution if needed
            table.append(pa_table)
            action = "appended"
            
        except NoSuchTableError:
            # Create Table
            print(f"Table '{full_table_name}' does not exist. Creating...")
            
            # Use custom inference instead of pyarrow_to_schema
            iceberg_schema = infer_iceberg_schema(pa_table.schema)
            print(f"Inferred Schema: {iceberg_schema}")
            
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
            "schema_summary": str(pa_table.schema)
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
        try:
            print("DEBUG: Listing tables in 'default' namespace...")
            tables = catalog.list_tables("default")
            print(f"DEBUG: Found tables: {tables}")
            
            for tbl in tables:
                if isinstance(tbl, tuple):
                     tbl_name = tbl[-1]
                else:
                     tbl_name = tbl.name 
                     
                fullname = f"default.{tbl_name}"
                print(f"DEBUG: Loading table '{fullname}'...")
                params = catalog.load_table(fullname)
                loc = params.metadata_location
                
                # Register View
                print(f"DEBUG: Registering view '{tbl_name}' -> {loc}")
                con.execute(f"CREATE OR REPLACE VIEW {tbl_name} AS SELECT * FROM iceberg_scan('{loc}')")
                
        except Exception as e:
            print(f"Warning during view registration: {e}")
            import traceback
            traceback.print_exc()
            
        # Run Query
        print(f"Executing SQL: {sql}")
        results = con.execute(sql).fetchdf()
        
        # Handle NaN/Inf for JSON serialization
        results = results.where(pd.notnull(results), None)
        
        return results.to_dict(orient="records")
        
    except Exception as e:
         print(f"Query Error: {e}")
         raise HTTPException(status_code=400, detail=str(e))

@app.post("/ingest/hudi")
async def ingest_hudi_table(
    table: str = Form(...),
    pkey: str = Form(...),
    file: UploadFile = File(...),
    partition: str = Form(None),
    precombine: str = Form(None)
):
    """
    Ingest data into Hudi table using the existing ingest_csv_hudi.py script.
    """
    try:
        # Define paths
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        script_path = os.path.join(base_dir, "hive_trino_setup", "ingest_csv_hudi.py")
        temp_dir = os.path.join(base_dir, "temp_uploads")
        
        # Ensure temp directory exists
        os.makedirs(temp_dir, exist_ok=True)
        
        # Save uploaded file
        file_ext = os.path.splitext(file.filename)[1]
        temp_filename = f"{uuid.uuid4()}{file_ext}"
        temp_file_path = os.path.join(temp_dir, temp_filename)
        
        print(f"DEBUG: Saving upload to {temp_file_path}")
        
        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        # Construct command
        command = [
            sys.executable,
            script_path,
            "--file", temp_file_path,
            "--table", table,
            "--pkey", pkey
        ]
        
        if partition:
            command.extend(["--partition", partition])
            
        if precombine:
            command.extend(["--precombine", precombine])
            
        print(f"DEBUG: Running command: {' '.join(command)}")
        
        # Run script
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            cwd=base_dir # Run from project root so relative paths in script might work if any (though we use absolute for script)
        )
        
        # Clean up temp file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            
        if result.returncode != 0:
            print(f"Script Error Output:\n{result.stderr}")
            raise HTTPException(status_code=500, detail=f"Ingestion script failed: {result.stderr}")
            
        return {
            "status": "success",
            "message": "Hudi ingestion complete",
            "output": result.stdout
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"API Error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/delete/hudi")
async def delete_hudi_records(
    table: str = Form(...),
    pkey: str = Form(...),
    ids: str = Form(...)
):
    """
    Delete records from a Hudi table.
    ids: Comma-separated list of primary keys to delete.
    """
    print(f"Request to delete IDs {ids} from {table}")
    try:
         # Define paths
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        # We need the delete script which I just created
        script_path = os.path.join(base_dir, "hive_trino_setup", "delete_hudi.py")
        
        if not os.path.exists(script_path):
            raise HTTPException(status_code=500, detail="Delete script not found on server")

        command = [
            sys.executable,
            script_path,
            "--table", table,
            "--pkey", pkey,
            "--ids", ids
        ]
        
        print(f"DEBUG: Running delete command: {' '.join(command)}")
        
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            cwd=base_dir
        )
        
        if result.returncode != 0:
            print(f"Delete Script Error:\n{result.stderr}")
            raise HTTPException(status_code=500, detail=f"Delete failed: {result.stderr}")
            
        return {
            "status": "success",
            "message": f"Successfully processed delete request for {table}",
            "output": result.stdout
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Delete API Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query/hudi")
def query_hudi_trino(sql: str):
    """
    Query Hudi tables using Trino.
    This is the preferred way to read Hudi data in this stack.
    """
    print(f"Executing Trino SQL: {sql}")
    try:
        conn = trino.dbapi.connect(
            host='localhost',
            port=8082,
            user='admin',
            catalog='hudi',
            schema='default'
        )
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        
        # Get column names
        columns = [desc[0] for desc in cur.description]
        
        # Convert to list of dicts
        result = [dict(zip(columns, row)) for row in rows]
        
        return result
        
    except Exception as e:
        print(f"Trino Query Error: {e}")
        # If Trino module is missing or connection fails
        raise HTTPException(status_code=500, detail=f"Trino Error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
