# Data Lake Demo Guide (Spark + Iceberg + Trino + Hudi)

This guide provides a step-by-step walkthrough to demonstrate the capabilities of your local Data Lakehouse.

---

## 🚀 Phase 1: Infrastructure Setup

**Goal:** Start the entire stack (MinIO, Trino, Hive Metastore, Postgres, FastAPI).

1.  **Navigate to the specialized setup directory:**
    ```bash
    cd hive_trino_setup
    ```

2.  **Start Docker Containers:**
    ```bash
    docker-compose down
    docker-compose up -d
    ```

3.  **Wait for health checks:**
    Wait about 30-60 seconds for Trino and Hive Metastore to initialize.
    *   Trino UI: [http://localhost:8082](http://localhost:8082) (User: admin)
    *   MinIO Console: [http://localhost:9862](http://localhost:9862) (Login: `minioadmin` / `minioadmin`)

4.  **Start the API Server (in a separate terminal):**
    ```bash
    # Ensure you are in the project root
    cd ~/Data_Lake_System
    source venv/bin/activate
    
    # Start API on port 8001 (to avoid conflict)
    # Note: Ensure api/main.py is configured to port=8001
    python api/main.py
    ```

---

## ❄️ Phase 2: Iceberg Ingestion (Spark)

**Goal:** Ingest a raw CSV file (`leads.csv`) into an Iceberg table using Spark.

1.  **Run Ingestion Command:**
    We will ingest `data/leads-100.csv` into a table named `default.leads`.

    ```bash
    python lake_cli.py ingest data/leads-100.csv default.leads --format iceberg
    ```

    *   **What happens?**
        *   The CLI sends the file to the API.
        *   The API triggers a **Spark Job**.
        *   Spark reads the CSV, infers the schema, and writes Parquet files to MinIO (`s3a://datalake/leads/data/...`).
        *   Spark registers the table in the Hive Metastore.

2.  **Verify in MinIO:**
    *   Go to MinIO Console -> Buckets -> `datalake`.
    *   Navigate to `leads/data/`. You should see `.parquet` files.

---

## 🔍 Phase 3: Querying Data (Trino)

**Goal:** Query the newly created Iceberg table using Trino via the CLI.

1.  **Read All Data:**
    ```bash
    python lake_cli.py read default.leads --format iceberg
    ```
    *Output: JSON data of the leads.*

2.  **Read with Columns & Limits:**
    ```bash
    python lake_cli.py read default.leads --columns "email_1,company" --limit 5 --format iceberg
    ```

---

## ⏳ Phase 4: Time Travel & History

**Goal:** Demonstrate Iceberg's metadata and history capabilities.

1.  **View Table History (Snapshots):**
    Query the metadata table `$history` to see committed snapshots.
    *(Note: Use just `leads` without `default.` prefix for system table lookups via this CLI)*

    ```bash
    python lake_cli.py read "leads\$history" --format iceberg
    ```
    *Output: List of snapshots with timestamps and IDs.*

---

## 🔥 Phase 5: Hudi Features (Upserts)

**Goal:** Demonstrate Hudi's ability to update records (ACID transactions).

1.  **Ingest Initial Data (Hudi):**
    ```bash
    python lake_cli.py ingest data/leads-100.csv hudi_leads --format hudi
    ```

2.  **Verify Initial Read:**
    ```bash
    python lake_cli.py read hudi_leads --limit 1
    ```

3.  **Perform an Upsert (Update):**
    Create a small update file (e.g., `update.csv`) with a changed value for an existing ID.
    ```bash
    # Example update file creation
    echo "index,first_name,last_name,email_1" > update.csv
    echo "1,Victor_UPDATED,Cochran,new_email@example.com" >> update.csv

    # Run Ingest again (Hudi handles it as Upsert)
    python lake_cli.py ingest update.csv hudi_leads --format hudi --pkey index
    ```

4.  **Verify Update:**
    Read the record again to see the changed name.
    ```bash
    python lake_cli.py read hudi_leads --where "index=1"
    ```

---

## 🛠 Troubleshooting

*   **Port Conflicts:** If `Address already in use` error:
    ```bash
    fuser -k -9 8000/tcp
    fuser -k -9 8001/tcp
    python api/main.py
    ```
*   **Permissions:** If `Permission denied` on `data/` folder:
    ```bash
    sudo chown -R $USER:$USER data/
    ```
