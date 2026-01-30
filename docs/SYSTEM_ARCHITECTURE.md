# Data Lake System Architecture

This document outlines the architecture of the local Data Lakehouse system, designed to support ACID transactions, time travel, and high-performance querying using open standards.

## 🏗️ High-Level Architecture Diagram

```mermaid
graph TD
    User[User / CLI] -->|REST API| API[FastAPI Gateway]
    
    subgraph "Compute Layer"
        API -->|Subprocess/Job| Spark[Apache Spark]
        API -->|JDBC/DBAPI| Trino[Trino Query Engine]
    end
    
    subgraph "Metadata Layer"
        Spark -->|Register Table| HMS[Hive Metastore]
        Trino -->|Fetch Schema| HMS
        HMS -->|Store Metdata| PG[PostgreSQL]
    end
    
    subgraph "Storage Layer"
        Spark -->|Write Parquet| MinIO[MinIO (S3)]
        Trino -->|Read Parquet| MinIO
    end

    classDef component fill:#f9f,stroke:#333,stroke-width:2px;
    classUser fill:#fff,stroke:#333;
```

## 🧩 Core Components

### 1. Client Layer
*   **Lake CLI (`lake_cli.py`)**: A Python-based command-line interface that allows users to ingest files, query data, and manage tables interactively.
*   **API Gateway (`api/main.py`)**: A FastAPI application that serves as the entry point for the system. It abstracts the complexity of the underlying engines from the user.

### 2. Compute Layer
*   **Apache Spark (v3.5)**: The heavy lifter for data ingestion. It handles reading raw CSVs, inferring schemas, and writing optimized Parquet files in **Iceberg** or **Hudi** formats.
*   **Trino (v466)**: A distributed SQL query engine used for high-speed data retrieval. It allows users to run standard SQL queries on data stored in MinIO without moving it.

### 3. Metadata Layer
*   **Hive Metastore (HMS)**: The central repository that stores table definitions (schema, partition info) and maps them to data locations in S3/MinIO. Both Spark and Trino sync with HMS to ensure a unified view of the data.
*   **PostgreSQL**: The backing database for the Hive Metastore.

### 4. Storage Layer
*   **MinIO**: A high-performance, S3-compatible object storage server.
    *   **Data**: Stored as `.parquet` files.
    *   **Formats**: Supports **Apache Iceberg** (for snapshots/time-travel) and **Apache Hudi** (for upserts).

---

## 🔄 Data Flows

### A. Data Ingestion Flow
1.  **User** sends a CSV file via CLI (`lake_cli.py ingest`).
2.  **API** saves the file temporarily and launches a **Spark Job**.
3.  **Spark**:
    *   Reads the CSV.
    *   Writes data to **MinIO** in Iceberg/Hudi format (`s3a://datalake/...`).
    *   Updates **Hive Metastore** with the new table or snapshot.

### B. Data Query Flow
1.  **User** requests data (`lake_cli.py read`).
2.  **API** connects to **Trino** via JDBC/Python DBAPI.
3.  **Trino**:
    *   Consults **Hive Metastore** for table locations.
    *   Reads optimized Parquet files directly from **MinIO**.
    *   Returns the result set to the API, then to the user.

---

## 🛠 Technology Stack

| Component | Technology | Docker Image |
| :--- | :--- | :--- |
| **Language** | Python 3.12 | N/A |
| **API Framework** | FastAPI | `python:3.12` |
| **Ingestion Engine** | Apache Spark | `apache/spark:3.5.3` |
| **Query Engine** | Trino | `trinodb/trino:466` |
| **Storage** | MinIO (S3) | `minio/minio` |
| **Metastore** | Hive Metastore | `apache/hive:3.1.3` |
| **Table Formats** | Apache Iceberg, Hudi | N/A |
