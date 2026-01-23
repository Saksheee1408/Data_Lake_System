import duckdb

def get_duckdb_connection():
    """
    Establishes a DuckDB connection and configures it for MinIO and Iceberg.
    """
    con = duckdb.connect(database=':memory:')
    
    # Install and load required extensions
    # Note: On some systems, explicit install might be needed mainly for the first time
    # ensure_loaded handles install if load fails usually, but let's be explicit
    extensions = ['httpfs', 'iceberg']
    for ext in extensions:
        con.execute(f"INSTALL {ext};")
        con.execute(f"LOAD {ext};")
    
    # Configure S3/MinIO Settings
    # These settings allow DuckDB to treat MinIO like S3
    con.execute("SET s3_endpoint='localhost:9000';")
    con.execute("SET s3_access_key_id='minioadmin';")
    con.execute("SET s3_secret_access_key='minioadmin';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_region='us-east-1';")
    con.execute("SET s3_url_style='path';")
    
    return con
