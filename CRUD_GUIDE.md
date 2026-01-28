# 🏄‍♂️ Data Lake CRUD Manual

This guide explains how to perform CRUD (Create, Read, Update, Delete) operations on your Data Lakehouse using the `lake_shell.py` tool. This tool mimics a local database experience.

## 🚀 Starting the Shell

Open a terminal and run:
```bash
python lake_shell.py
```

You will see the `(lake)` prompt.

## 1. CREATE (Insert Data)
Insert new records using the `insert` command.
**Syntax**: `insert <table> <pkey_column> <pkey_value> <col>=<val> ...`

```bash
(lake) insert users id 1 name="Alice" role="engineer" salary="100000"
(lake) insert users id 2 name="Bob" role="manager" salary="120000"
```

## 2. READ (Select Data)
View data primarily using `select` or `get`.

**List all rows**:
```bash
(lake) select users
```

**Get a specific record**:
```bash
(lake) get users id 1
```

**Run complex SQL**:
```bash
(lake) sql SELECT * FROM hudi.default.users WHERE role = 'engineer'
```

## 3. UPDATE (Modify Data)
Modify existing records safely. The shell reads the current state, applies your changes, and writes a new version (Upsert).

**Syntax**: `update <table> <pkey_column> <pkey_value> <col>=<new_val>`

**Example: Give Alice a raise**:
```bash
(lake) update users id 1 salary="110000"
```

**Example: Promote Bob**:
```bash
(lake) update users id 2 role="director"
```

## 4. DELETE (Remove Data)
Remove records permanently.

**Syntax**: `delete <table> <pkey_column> <pkey_value>`

```bash
(lake) delete users id 2
```

## ℹ️ Concept Mapping

| SQL Concept | Lake Shell Command | Backend Operation |
|-------------|--------------------|-------------------|
| `INSERT`    | `insert`           | `ingest/hudi` (Append/Upsert) |
| `SELECT`    | `select` / `get`   | `hudi/read` API |
| `UPDATE`    | `update`           | Read -> Patch -> `ingest/hudi` |
| `DELETE`    | `delete`           | `delete/hudi` API |
