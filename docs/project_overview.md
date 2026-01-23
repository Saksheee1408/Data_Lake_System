# Project Overview: Local Data Lakehouse

## 📖 Introduction
This project implements a fully functional **Data Lakehouse** that runs entirely on a local machine or a single server. It combines the low-cost storage of a Data Lake (MinIO) with the reliability and performance of a Data Warehouse (Iceberg + DuckDB), accessible via a modern REST API.

## ✨ Key Features
1.  **ACID Transactions**: Data integrity is guaranteed. Reads never block writes.
2.  **Schema Evolution**: Add columns to your tables without breaking existing queries or rewriting content.
3.  **Time Travel**: Query your data as it looked at any point in the past.
4.  **Dynamic Ingestion**: Upload *any* CSV, and the system automatically infers the schema and creates the table.
5.  **SQL Interface**: Query all your data using standard SQL via DuckDB.
6.  **REST API**: Expose all functionality via HTTP endpoints, making it easy to integrate with web apps or other services.

## 🔄 User Flow
The system follows a streamlined "Ingest → Store → Query" flow:

1.  **Client (User/App)** uploads a CSV file to `POST /ingest/{table_name}`.
2.  **FastAPI Backend** receives the file and converts it to PyArrow format.
3.  **Schema Inference Engine** detects column types and creates an Apache Iceberg Table in the Catalog if it doesn't exist.
4.  **MinIO (Storage)** stores the physical data files (Parquet) and metadata strings.
5.  **DuckDB Engine** allows users to request data via `GET /query`. It reads the Iceberg metadata and executes high-performance analytical queries.

## 🏗️ System Components
| Component | Function |
|-----------|----------|
| **MinIO** | **Storage Layer**. S3-compatible object storage that holds all Parquet files. |
| **Apache Iceberg** | **Table Format**. Manages metadata, snapshots, schemas, and file tracking. |
| **DuckDB** | **Compute Engine**. Executes SQL queries efficiently on the local machine. |
| **FastAPI** | **Interface Layer**. A Python web server exposing the Lakehouse to the outside world. |
