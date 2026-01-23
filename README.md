# Local Data Lakehouse (MinIO + Iceberg + DuckDB + FastAPI)

A lightweight, local Data Lakehouse implementation demonstrating ACID transactions, Time Travel, and Schema Evolution on a laptop.

## 🏗️ Architecture

| Component | Technology | Role |
|-----------|------------|------|
| **Storage** | **MinIO** | S3-compatible Object Storage (runs in Docker). |
| **Table Format** | **Apache Iceberg** | Provides ACID transactions, Schema Evolution, and Time Travel. |
| **Query Engine** | **DuckDB** | Fast analytical SQL engine to read/write data. |
| **API Layer** | **FastAPI** | REST API for Ingestion and Querying. |

## 🚀 Prerequisites

1.  **Docker Desktop** (for MinIO)
2.  **Python 3.9+**
3.  **Git**

## 🛠️ Setup Guide

### 1. Clone & Install Dependencies
```bash
git clone https://github.com/Saksheee1408/Data_Lake_System.git
cd Data_Lake_System
pip install -r requirements.txt
pip install -r api/requirements.txt
```

### 2. Start Infrastructure
Start the MinIO object storage:
```bash
docker-compose up -d
```
*   **Console**: [http://localhost:9001](http://localhost:9001) (User/Pass: `minioadmin`)
*   **API**: [http://localhost:9000](http://localhost:9000)

### 3. Initialize Bucket
Create the `warehouse` bucket:
```bash
python setup_infra.py
```

## 🏃 Usage: Scripts (CLI)

Run these scripts in order to verify core Lakehouse functionality.

| Step | Script | Description |
|------|--------|-------------|
| 1 | `python create_table.py` | Creates `default.sales` Iceberg table. |
| 2 | `python ingest_data.py` | Inserts sample records (Phase 1 data). |
| 3 | `python read_data.py` | Reads data using DuckDB. |
| 4 | `python update_data.py` | Performs a Copy-on-Write UPDATE. |
| 5 | `python schema_evolve.py` | Adds a `channel` column and inserts new data. |
| 6 | `python time_travel.py` | Queries historical snapshots (Time Travel). |

## 🌐 Usage: Backend API

Expose the Lakehouse via a REST API.

### 1. Start the Server
```bash
cd api
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```
*   **Swagger Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)

### 2. Test the API End-to-End
Open a new terminal (keep uvicorn running) and run the automated test:
```bash
python test_api.py
```
This script will:
1.  Generate a test CSV.
2.  POST it to `/upload`.
3.  GET `/query` to verify data availability.

## 📂 Project Structure

```
├── api/
│   ├── main.py              # FastAPI Application
│   └── requirements.txt     # API Dependencies
├── docker-compose.yml       # MinIO Service Definition
├── setup_infra.py           # MinIO Bucket Setup
├── db_connection.py         # DuckDB Connection Helper
├── create_table.py          # Iceberg DDL
├── ingest_data.py           # Iceberg Write (Batch)
├── read_data.py             # DuckDB Read
├── update_data.py           # CoW Update Example
├── schema_evolve.py         # Schema Evolution Example
├── time_travel.py           # Time Travel Example
├── test_api.py              # API Verification Script
└── requirements.txt         # Core Dependencies
```

## 📝 Design Notes
- **Catalog**: Uses a local SQLite database (`iceberg_catalog.db`) to track table state.
- **Copy-on-Write**: Deletes and Updates rewrite data files to ensure atomicity (Iceberg V1/V2 spec).
- **Concurrency**: DuckDB is embedded; for high concurrency, consider running it in read-only mode or using a catalog service like Nessie.
