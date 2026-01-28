# Implementation Details

## 🛠️ Technical Architecture

### 1. Storage & Catalog
*   **MinIO**: Deployed via Docker Compose (`docker-compose.yml`), mapping port `9000` for API access.
*   **Catalog**: We utilize **Hive Metastore** (running in Docker) as the Iceberg catalog. This allows interoperability between Spark, Trino, and self-managed Iceberg clients (FastAPI).
*   **Query Engine**: **Trino** is used for distributed SQL querying, while **DuckDB** is used for lightweight, local analysis within the API.

### 2. The Ingestion Pipeline (`api/main.py`)
The `ingest_table_dynamic` endpoint is the core of the write path:
*   **Input**: Streamed CSV file via `UploadFile`.
*   **Processing**:
    1.  Pandas reads the stream (`pd.read_csv`) to infer types.
    2.  Converted to `pyarrow.Table`.
    3.  **Custom Schema Mapper**: A specific function `infer_iceberg_schema` maps Arrow types to Iceberg types.
*   **Action**:
    *   Interacts with the Hive Metastore to Create or Append to the table.
    *   Data is written to MinIO (`s3a://warehouse/iceberg/`).

### 3. The Query Engine (`api/main.py`)
The API exposes endpoints to query data via:
*   **DuckDB**: For internal, fast, in-process queries.
*   **Trino**: For distributed, scalable SQL queries over the Data Lake.

## 📂 Code Structure
*   `api/`: Contains the FastAPI application.
    *   `main.py`: The single-file entry point for the backend logic.
*   `spark_jobs/`: Spark scripts for large-scale batch ingestion.
*   `hive_trino_setup/`: Configuration for the Hive Metastore and Trino environment.
*   `data/`: Data files (CSVs).
*   `archive/`: Legacy scripts used for initial architecture testing (`create_table.py`, etc.).
*   `lake_cli.py`: A CLI tool to interact with the API.

## 🔐 Configuration
*   **S3 Endpoint**: `http://localhost:9000` (Local MinIO).
*   **Credentials**: `minioadmin` / `minioadmin` (Default).
*   **Region**: `us-east-1` (MinIO default).
