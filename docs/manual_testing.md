# 🛠️ Manual Testing Guide (The "No GUI" Approach)

If UI tools are failing due to firewalls/timeouts, use the terminal. This provides the ground truth.

## Scenario A: Testing ON the Server (The "Is it broken?" Test)
Run these commands inside your SSH session on the Ubuntu server. If these fail, the API is down.

### 1. Health Check
```bash
curl http://localhost:8000/
# Expected: {"status":"ok","message":"Lakehouse API is running"}
```

### 2. Ingest Data
Create a dummy file and upload it.
```bash
# Create file
echo "id,name,cost" > test.csv
echo "1,Widget,10.0" >> test.csv

# Upload
curl -X POST -F "file=@test.csv" http://localhost:8000/ingest/manual_test_table
```

### 3. Query Data
```bash
curl "http://localhost:8000/query?sql=SELECT*FROM%20manual_test_table"
```

---

## Scenario B: Testing from Your Laptop (The "Connectivity" Test)
Run these from your Windows PowerShell or Git Bash.

### 1. Open the Tunnel (CRITICAL)
If you haven't already:
```bash
ssh -L 8000:localhost:8000 ubuntu@172.203.228.211
```
*Leave this window OPEN. It is your lifeline.*

### 2. Run Health Check (Localhost)
Open a **new** terminal window:
```bash
curl http://localhost:8000/
```
*If this works, the tunnel is alive.*

### 3. Query via Tunnel
```bash
curl "http://localhost:8000/query?sql=SELECT*FROM%20manual_test_table"
```

---

## Scenario C: Bulk Data Test (Companies)
Let's test with a larger dataset (10 rows) to see the schema inference in action.

### 1. Create the Data
Run this in your terminal to create `companies.csv` with 10 top companies:

```bash
# Create the CSV file
cat <<EOF > companies.csv
rank,name,industry,valuation_billions,founded_year
1,Apple,Technology,3000,1976
2,Microsoft,Technology,2500,1975
3,Saudi Aramco,Energy,2000,1933
4,Alphabet,Technology,1500,1998
5,Amazon,Retail,1300,1994
6,Nvidia,Technology,1200,1993
7,Tesla,Automotive,800,2003
8,Meta,Technology,700,2004
9,Berkshire Hathaway,Finance,700,1839
10,TSMC,Semiconductors,500,1987
EOF
```

### 2. Ingest Data
This will create a new Iceberg table named `companies`.

```bash
curl -X POST -F "file=@companies.csv" http://localhost:8000/ingest/companies
# Expected Output: "Successfully created data to default.companies", "rows": 10
```

### 3. Query Data
Let's find all **Technology** companies founded after **1990**:

```bash
curl "http://localhost:8000/query?sql=SELECT*FROM%20companies%20WHERE%20industry='Technology'%20AND%20founded_year>1990"
```

