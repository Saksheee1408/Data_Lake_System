# Data Lakehouse API Usage Guide

This guide details how to perform CRUD (Create, Read, Update, Delete) operations on your Data Lakehouse using simple Python `requests`.

## 1. Create / Ingest (Upload a New File)
To ingest data, upload a CSV file.
- **Endpoint:** `POST /ingest/hudi`
- **Parameters:**
  - `table`: Name of the table (e.g., `employees`)
  - `pkey`: Primary Key column name (e.g., `id`)
  - `file`: The CSV file path
  - `partition`: (Optional) Column to partition by (e.g., `dept`)

```python
import requests

url = "http://localhost:8000/ingest/hudi"
files = {'file': open('my_data.csv', 'rb')}
data = {
    'table': 'my_table',
    'pkey': 'id',
    'partition': 'category' # Optional
}

response = requests.post(url, files=files, data=data)
print(response.json())
```

```

## 1.1 Ingest via Spark (Iceberg)
To ingest data into an Iceberg table using Spark explicitly:
- **Endpoint:** `POST /ingest/iceberg/spark`
- **Parameters:**
  - `table`: Name of the table (e.g., `default.sales`)
  - `file`: The CSV file path
  - `mode`: `append` (default) or `overwrite`

```python
import requests

url = "http://localhost:8000/ingest/iceberg/spark"
files = {'file': open('sales.csv', 'rb')}
data = {
    'table': 'default.sales',
    'mode': 'append'
}

response = requests.post(url, files=files, data=data)
print(response.json())
```

## 2. Update (Modify Data)
To update existing records, simply **ingest the same CSV** (or a new CSV containing just the rows to change) with the **same Primary Keys**. Hudi handles the "Upsert" automatically.

```python
# To update ID 101's salary to 90000:
# Create a small CSV with just that row
csv_update = "id,name,category,salary\n101,John Doe,Engineering,90000"

files = {'file': ('update.csv', csv_update, 'text/csv')}
data = {'table': 'my_table', 'pkey': 'id', 'partition': 'category'}

response = requests.post(url, files=files, data=data)
print(response.json())
```

## 3. Read (Query Data)
You can read data using simple parameters without writing SQL.
- **Endpoint:** `GET /hudi/{table}/read`

```python
# Read all data
url = "http://localhost:8000/hudi/my_table/read"
resp = requests.get(url)
print(resp.json())

# Read specific columns
resp = requests.get(url, params={"columns": "id,name,salary"})

# Read with filter
resp = requests.get(url, params={"filter_col": "category", "filter_val": "Engineering"})
```

## 4. Delete (Remove Records)
To delete specific records, provide their Primary Keys.
- **Endpoint:** `POST /delete/hudi`

```python
url = "http://localhost:8000/delete/hudi"
data = {
    'table': 'my_table',
    'pkey': 'id',
    'ids': '101,103'  # Comma-separated list of IDs to delete
}

response = requests.post(url, data=data)
print(response.json())
```
