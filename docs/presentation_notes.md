# Local Data Lakehouse Project Presentation

## 1. Project Overview
We have built a **Local Data Lakehouse**—a modern data architecture that combines the flexibility of a data lake with the reliability of a data warehouse. This system runs improved data storage capabilities locally on a standard machine, supporting ACID transactions, schema evolution, and time travel.

The core goal was to create a functional, local version of enterprise data platforms (like Databricks or Snowflake) to demonstrate advanced data engineering concepts without needing expensive cloud infrastructure.

---

## 2. What We Have Done (Key Achievements)

### ✅ Infrastructure Setup
- Deployed **MinIO** (S3-compatible storage) using Docker containers to simulate cloud storage.
- Configured a local **Iceberg Catalog** (using SQLite) to manage table metadata.

### ✅ Core Data Operations (CRUD + Advanced)
We implemented Python scripts for the full lifecycle of data management:
- **Create**: Defined strong schemas for tables (e.g., Sales table).
- **Ingest**: Loaded data from CSVs into Iceberg tables.
- **Read**: Queried data using **DuckDB** for high-performance analytics.
- **Update/Delete**: Implemented "Upsert" logic to modify existing records and delete obsolete ones.

### ✅ Advanced Data Engineering Features
- **Schema Evolution**: Successfully added new columns (e.g., `discount`) to existing tables without breaking pipelines.
- **Time Travel**: Implemented functionality to query the database as it existed at a specific point in the past (using snapshots).

### ✅ API & Integration
- Built a **FastAPI** application to serve as the gateway.
- Endpoints allow users to upload CSV files (Ingest) and run SQL queries (Consumption) via HTTP requests.
- Integrated **DuckDB** dynamically to read Iceberg tables directly from MinIO storage.

---

## 3. Technology Stack

| Component | Technology | Role in Project |
|-----------|------------|-----------------|
| **Storage Layer** | **MinIO** | Acts as our "S3 Bucket" local storage. Stores the actual Parquet data files and metadata. |
| **Table Format** | **Apache Iceberg** | Provides the open table format. Enables ACID transactions, schema evolution, and time travel on top of MinIO files. |
| **Metadata Catalog**| **SQLite** | A lightweight SQL database used to store the pointers to current Iceberg table metadata. |
| **Compute Engine**| **DuckDB** | An in-process SQL OLAP database. Used to execute high-speed SQL queries against the Iceberg tables. |
| **API Layer** | **FastAPI** | Python web framework used to create the REST API for ingesting data and running queries. |
| **Data Processing**| **PyArrow & Pandas** | Used for in-memory data manipulation and Type conversion before writing to Iceberg. |
| **Containerization**| **Docker** | Used to run the MinIO server in an isolated environment. |
| **Testing** | **Postman** | Used to verify API endpoints and simulate client requests. |

---

## 4. Purpose of Every File

### 📂 Root Directory
| File Name | Purpose |
|-----------|---------|
| `docker-compose.yml` | **Infrastructure Config**. Defines the MinIO Docker service, ports (9000/9001), and credentials. |
| `setup_infra.py` | **Initialization Script**. Connects to MinIO and creates the required `warehouse` bucket if it doesn't exist. |
| `requirements.txt` | **Dependencies**. Lists all Python libraries needed (e.g., `fastapi`, `pyiceberg`, `duckdb`, `boto3`). |
| `db_connection.py` | **Helper Utility**. Contains the `get_duckdb_connection()` function to configuring DuckDB with MinIO credentials. |
| `test_api.py` | **Automated Tests**. A script that sends requests to the running API to verify Ingest and Query functionality works end-to-end. |

### 🐍 Core Operation Scripts
| File Name | Purpose |
|-----------|---------|
| `create_table.py` | Initializes the `sales` table in the Iceberg catalog with a specific schema (id, amount, date). |
| `ingest_data.py` | Generates sample data and **appends** it to the `sales` table. |
| `read_data.py` | Reads the `sales` table and prints the current data to the console using DuckDB. |
| `update_data.py` | Demonstrates **Upserts**. Modifies specific records (e.g., updating an amount) based on an ID. |
| `delete_data.py` | Removes records from the table based on a condition (e.g., "id = 1"). |
| `time_travel.py` | Demonstrates accessing historical data. Queries the table state at a specific Snapshot ID from the past. |
| `schema_evolve.py`| Demonstrates **Schema Evolution**. Adds a new column (e.g., `profit`) to the table structure dynamically. |

### 📂 /api Directory
| File Name | Purpose |
|-----------|---------|
| `main.py` | **The Web Server**. Initializes the FastAPI app. Defines endpoints: <br>1. `POST /ingest/{table}`: Uploads CSV data.<br>2. `GET /query`: Runs SQL queries against the Data Lake. |

### 📂 /docs Directory
| File Name | Purpose |
|-----------|---------|
| `manual_testing.md` | A checklist and guide for manually verifying that the system is creating files and responding to queries correctly. |
| `project_overview.md` | High-level conceptual documentation of the project. |
