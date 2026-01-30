# Demo Commands Cheat Sheet

## 1. Setup & Start
```bash
# Start Infrastructure
cd hive_trino_setup
docker-compose down
docker-compose up -d

# Start API (Port 8001)
cd ~/Data_Lake_System
source venv/bin/activate
fuser -k -9 8001/tcp
python api/main.py &
```

## 2. Iceberg Ingestion (Spark)
```bash
# Ingest CSV into Iceberg Table
python lake_cli.py ingest data/leads-100.csv default.leads --format iceberg
```

## 3. Query Data (Trino)
```bash
# Read all data
python lake_cli.py read default.leads --format iceberg

# Read specific columns
python lake_cli.py read default.leads --columns "email_1,company" --limit 5 --format iceberg
```

## 4. Time Travel (History)
```bash
# View History / Snapshots
python lake_cli.py read "leads\$history" --format iceberg
```

## 5. Hudi Ingestion & Update
```bash
# Ingest Hudi Table
python lake_cli.py ingest data/leads-100.csv hudi_leads --format hudi

# Create Update Payload
echo "index,first_name,last_name" > update.csv
echo "1,VICTOR_UPDATED,Cochran" >> update.csv

# Upsert (Update Record)
python lake_cli.py ingest update.csv hudi_leads --format hudi --pkey index

# Verify Update
python lake_cli.py read hudi_leads --where "index=1"
```

## 6. Access URLs
*   **MinIO Console:** [http://localhost:9862](http://localhost:9862) (User/Pass: `minioadmin`)
*   **Trino UI:** [http://localhost:8082](http://localhost:8082) (User: `admin`)
