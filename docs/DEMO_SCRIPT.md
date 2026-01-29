# 🎯 Data Lakehouse Demo Script

This document provides a step-by-step "Script" to demonstrate the capabilities of the Local Data Lakehouse system. Follow these commands in order to show the full lifecycle of data: Ingestion, Analysis, Modification, and Audit (Time Travel).

## PREREQUISITES
Ensure your infrastructure is running.
```bash
# Terminal 1: Infrastructure
docker-compose up -d

# Terminal 2: API
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

---

## 🏁 PHASE 1: START THE SHELL
Open a new terminal to act as the "Client".

```bash
python lake_shell.py
```
*You verify the prompt changes to `(lake)`.*

---

## 📥 PHASE 2: INGESTION (Create)
**Objective**: Load a raw CSV file into the Lakehouse as a managed ACID table.

**Command**:
```bash
load_csv leads-100.csv leads Index
```
*   **Action**: Uploads `leads-100.csv`, creates a Hudi table named `leads`, and sets `Index` as the Primary Key.
*   **Talking Point**: "Notice how we just turned a raw CSV into a managed database table instantly."

---

## 🔍 PHASE 3: ANALYSIS (Read)
**Objective**: Query the data immediately.

**Command 1 (Simple View)**:
```bash
select leads 5
```
*   **Action**: Fetches top 5 rows.
*   **Talking Point**: "Data is instantly queryable via our API."

**Command 2 (Specific Record)**:
```bash
get leads index 2
```
*   **Action**: Fetches the record for "Andres".
*   **Note**: Observe the Name (`Andres Callahan`) and Company (`Crosby Inc`).

---

## ✏️ PHASE 4: UPDATE (Upsert)
**Objective**: Demonstrate ACID updates on the Data Lake (something traditional Data Lakes *cannot* do).

**Scenario**: The lead "Andres" (Index 2) has updated his name and switched companies.

**Command**:
```bash
insert leads Index 2 "Lead Id"="e9AABddbCA4AFee" "First Name"="New Andres" "Last Name"="Fuentes" Company="Tech Corp" "Phone 1"="001-672-799-2170x5610" "Phone 2"="+1-887-577-1205x5686" "Email 1"="new.email@tech.com" "Email 2"="hunter84@jefferson.com" Website="https://www.tech-corp.biz/" Source="Google Ads" "Deal Stage"="Closed Won" Notes="Promoted."
```
*   **Action**: Performs an Upsert (Update) on the existing record with Index 2.
*   **Talking Point**: "We are strictly updating the existing row, not appending a duplicate."

**Verification**:
```bash
get leads index 2
```
*   **Result**: Name should be "New Andres" and Email "new.email@tech.com".

---

## 🗑️ PHASE 5: DELETE (Hard Delete)
**Objective**: Remove a record permanently (e.g., GDPR request).

**Command**:
```bash
delete leads Index 1
```
*   **Action**: Removes the record with Index 1 ("Bethany Dixon").

**Verification**:
```bash
get leads index 1
```
*   **Result**: Should return empty or "Record not found".

---

## ⏳ PHASE 6: TIME TRAVEL (Audit)
**Objective**: The "Wow" Factor. Recover the data we just changed or deleted.

**Step 1: Check History**
```bash
history leads
```
*   **Action**: Lists all commit timestamps.
*   **Talking Point**: "Every transaction is tracked. We can see the Initial Load, the Update, and the Delete."
*   **Action**: **Copy** the timestamp of the *First Commit* (the bottom one).

**Step 2: Travel to the Past**
```bash
travel leads <PASTE_FIRST_TIMESTAMP_HERE>
```

**What to look for**:
1.  **Index 1 (Bethany)**: She is BACK! (Despite being deleted in the present).
2.  **Index 2 (Andres)**: His name is "Andres" again (Reverted from "New Andres").

---

## 🛑 OPTIONAL: SQL POWER
**Objective**: Show that this is real SQL, not just a toy script.

**Command**:
```bash
sql SELECT count(*), company FROM hudi.default.leads GROUP BY company HAVING count(*) > 1
```
*   **Action**: Runs a distributed aggregation via Trino.
