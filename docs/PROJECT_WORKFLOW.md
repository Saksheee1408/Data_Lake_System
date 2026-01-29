# Project Workflow & Implementation Guide

## 1. Project Workflow Overview

This document details the step-by-step workflow of data movement within the system, from the user's keystroke in the CLI to the underlying file storage.

### Flow Diagram
1.  **Input**: User runs a command in `lake_shell.py` (CLI).
2.  **Transport**: CLI sends an HTTP Request to `api/main.py` (FastAPI).
3.  **Processing**:
    *   **Write**: API triggers a `subprocess` call to run `ingest_csv_hudi.py` (Spark Job).
    *   **Read**: API connects to Trino via `trino-python-client` to execute SQL.
4.  **Storage/Execution**:
    *   Spark writes Parquet/Hudi logs to MinIO.
    *   Trino reads those files from MinIO.

---

## 2. Detailed Data Lifecycle

### Phase 1: Ingestion (Create/Insert)
**Scenario**: User runs `load_csv data/leads.csv leads Index`.

1.  **CLI**: Reads the local file and POSTs it to `http://server:8000/ingest/hudi`.
2.  **API**: 
    *   Saves the file to a temporary directory.
    *   Constructs a `spark-submit` command.
    *   Executes: `python hive_trino_setup/ingest_csv_hudi.py --file temp.csv --table leads --pkey Index ...`
3.  **Spark Job**:
    *   Starts a local Spark Session with Hudi jars loaded.
    *   Reads `temp.csv`.
    *   Checks Hive Metastore: "Does table `default.leads` exist?"
    *   **No**: Infers schema from CSV -> Creates table in Hive -> Writes initial Parquet files to `s3a://warehouse/leads`.
    *   **Yes**: Validates schema -> Appends data.
    *   **Commit**: Writes a `commit` file to `.hoodie` folder marking the transaction successful.

### Phase 2: Querying (Read)
**Scenario**: User runs `select leads 5`.

1.  **CLI**: Sends GET request to `/hudi/leads/read?limit=5`.
2.  **API**: Constructs SQL: `SELECT * FROM leads LIMIT 5`.
3.  **Trino Connection**: API connects to Trino (Port 8082).
4.  **Trino Engine**:
    *   Parses SQL.
    *   Asks Hive Metastore: "Where is specific `leads` table data?"
    *   HMS replies: "`s3a://warehouse/leads`".
    *   Trino scans the Hudi metadata to filter out deleted/old records.
    *   Returns the latest view of the data.
5.  **Response**: JSON data flows back -> API -> CLI (displayed as table).

### Phase 3: Updates (CRUD)
**Scenario**: User runs `update leads Index 1 name="New Name"`.

1.  **CLI**: The "Shell" logic kicks in.
    *   Existing data is fetched via API (Read Phase).
    *   The record with `Index=1` is modified in Python memory.
    *   A new "mini-CSV" containing just this updated row is generated.
2.  **Ingestion**: This mini-CSV is sent to the Ingestion API (same as Phase 1).
3.  **Spark Job (Upsert)**:
    *   Spark sees the incoming Row has `Index=1`.
    *   It checks the existing data in MinIO.
    *   It finds an existing file containing `Index=1`.
    *   **Copy-On-Write**: It reads that file, updates the record in memory, and writes a **NEW** version of that file.
    *   The old file is kept (for Time Travel) but marked as superseded in the metadata.

### Phase 4: Time Travel
**Scenario**: User runs `travel leads 202601280800`.

1.  **CLI**: Sends SQL: `SELECT * FROM leads WHERE "_hoodie_commit_time" <= '202601280800'`.
2.  **Trino**:
    *   Scans the table.
    *   Instead of just returning the "latest", it filters based on the hidden commit timestamp column.
    *   Returns records that match the version predicate.

---

## 3. Directory Structure & File Purpose

| Directory/File | Purpose |
|----------------|---------|
| `api/main.py` | **The Gateway**. The only entry point for external interaction. Translates HTTP requests into Spark jobs or Trino queries. |
| `lake_shell.py` | **The Client**. A user-friendly "Database Shell" that abstracts the API calls into SQL-like commands. |
| `hive_trino_setup/` | **Infrastructure Config**. Contains the Docker setup for the "Brain" (Hive) and the "Engine" (Trino). |
| `hive_trino_setup/ingest_csv_hudi.py` | **The Worker**. This is likely the most critical file. It contains the logic to convert raw CSVs into ACID transactional Hudi tables. |
| `docker-compose.yml` | **The Infrastructure**. Defines the 4 core services: MinIO (Storage), Spark (Write Compute), Hive (Metadata), Trino (Read Compute). |
| `data/` | **Raw Data**. A holding area for local CSVs before they are ingested. |
| `tests/` | **Verification**. Scripts to automatedly test that the API is functioning correctly. |

## 4. How to Scale (Future Proofing)
Currently, this runs on a single node (your laptop/server). However, the architecture is **Cloud Native**:
1.  **Storage**: Switch MinIO to **AWS S3** or **Azure Blob**. (Change 1 config line).
2.  **Compute**: Run Spark on **Databricks** or **EMR**.
3.  **Query**: Run Trino on a **Kubernetes Cluster**.
4.  **Catalog**: Switch Hive Metastore to **AWS Glue**.

The code (`ingest_csv_hudi.py` and `main.py`) requires **zero logic changes** to move from this local demo to a Petabyte-scale production environment.
