# Implementation Details

## 🛠️ Technical Architecture

### 1. Storage & Catalog
*   **MinIO**: Deployed via Docker Compose (`docker-compose.yml`), mapping port `9000` for API access.
*   **Catalog**: We utilize a lightweight SQL Catalog (`sqlite:///iceberg_catalog.db`). This file resides in the project root and tracks the location of the latest Metadata JSON for each table. This eliminates the need for a heavy catalog service like Nessie or Hive Metastore for this scale.

### 2. The Ingestion Pipeline (`api/main.py`)
The `ingest_table_dynamic` endpoint is the core of the write path:
*   **Input**: Streamed CSV file via `UploadFile`.
*   **Processing**:
    1.  Pandas reads the stream (`pd.read_csv`) to infer types.
    2.  Converted to `pyarrow.Table`.
    3.  **Custom Schema Mapper**: A specific function `infer_iceberg_schema` maps Arrow types (Int64, String, etc.) to Iceberg types and assigns unique Field IDs. This was crucial to bypass strict validation errors.
*   **Action**:
    *   If table exists: `table.append(data)`.
    *   If new: `catalog.create_table(...)` then `append`.

### 3. The Query Engine (`api/main.py`)
The `run_query` endpoint provides a "Serverless SQL" experience:
*   On every request, it spins up an ephemeral **DuckDB** in-memory connection.
*   **Dynamic View Registration**: It lists *all* tables in the Iceberg catalog and registers them as DuckDB Views (`CREATE VIEW x AS iceberg_scan(...)`).
*   This allows the user to write simple SQL (`SELECT * FROM products`) without knowing the underlying S3 paths.

## 📂 Code Structure
*   `api/`: Contains the FastAPI application.
    *   `main.py`: The single-file entry point for the backend logic.
*   `data/`: Local storage for MinIO (mapped volume).
*   `*.py` (Root): Standalone utility scripts (`create_table.py`, `time_travel.py`) used for initial architecture testing and debugging.

## 🔐 Configuration
*   **S3 Endpoint**: `http://localhost:9000` (Local MinIO).
*   **Credentials**: `minioadmin` / `minioadmin` (Default).
*   **Region**: `us-east-1` (MinIO default).
