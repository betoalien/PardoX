# Input / Output Operations

The bottleneck in most data pipelines is not computation, but **IO**. PardoX moves all data ingestion and persistence entirely into the Rust core, bypassing Python's file handling and object creation overhead.

---

## 1. CSV Files

PardoX features a **multi-threaded CSV reader**. The file is memory-mapped and parallel workers parse chunks simultaneously. No Python objects are created during ingestion.

### Basic usage

```python
import pardox as px

# Automatic type inference (Int64, Float64, Utf8)
df = px.read_csv("dataset.csv")
print(df.shape)    # (rows, cols)
print(df.columns)  # ['col1', 'col2', ...]
```

### Manual schema

```python
df = px.read_csv("dataset.csv", schema={
    "price":    "Float64",
    "quantity": "Float64",
    "id":       "Int64",
    "name":     "Utf8",
})
```

!!! info "Intelligent type inference"
    Without a schema, the engine scans the first rows to detect `Int64`, `Float64`, or `Utf8`. Pass a schema to override for specific columns.

!!! success "Parallel parsing"
    The file is split into logical blocks processed by multiple CPU cores concurrently.

---

## 2. Native Format (`.prdx`)

The **PRDX format** is a custom binary layout designed for instant persistence. Reading is a near-direct memory copy — no parsing, no schema detection.

### Save

```python
df.to_prdx("output.prdx")
```

### Load

```python
df = px.read_prdx("output.prdx")
```

!!! success "Benchmark"
    In tests with 10 GB datasets, reading a `.prdx` file achieves **~4.6 GB/s** throughput, limited only by NVMe SSD speed.

| Format | Read speed (2 GB file) |
|--------|----------------------|
| CSV | ~8s |
| Parquet | ~3s |
| **PRDX** | **~0.5s** |

---

## 3. Apache Arrow Bridge

PardoX integrates with the Arrow ecosystem via the **Arrow C Data Interface**. Conversion passes memory pointers — no data is copied.

```python
import pyarrow as pa
import pardox as px

arrow_table = pa.Table.from_pydict({"price": [1.5, 2.0], "qty": [10, 20]})
df = px.from_arrow(arrow_table)
```

!!! tip "Interoperability"
    The Arrow bridge connects PardoX to Polars, DuckDB, Apache Spark, and any other tool that supports the Arrow IPC format.

---

## 4. PostgreSQL

PardoX uses `tokio-postgres` inside the Rust core. The host language (Python / Node.js / PHP) never touches the database wire protocol.

### Connection string

```
postgresql://user:password@host:port/database
```

### Read

```python
from pardox.io import read_sql

df = read_sql(
    "postgresql://pardox:secret@localhost:5432/mydb",
    "SELECT id, amount, region FROM orders WHERE status = 'complete'"
)
```

### Execute DDL / DML

```python
from pardox.io import execute_sql

execute_sql(CONN, "DROP TABLE IF EXISTS orders_archive")
execute_sql(CONN, """
    CREATE TABLE orders_archive (
        id      BIGINT,
        amount  DOUBLE PRECISION,
        region  TEXT
    )
""")
# Returns 0 for DDL, affected rows for DML
n = execute_sql(CONN, "DELETE FROM orders WHERE status = 'cancelled'")
```

### Write

```python
# mode='append' — auto-activates COPY FROM STDIN for > 10,000 rows
rows = df.to_sql(CONN, "orders_archive", mode="append")
print(f"{rows:,} rows written")

# mode='upsert' — INSERT … ON CONFLICT (cols) DO UPDATE SET …
rows = df.to_sql(CONN, "orders_archive", mode="upsert", conflict_cols=["id"])
```

!!! success "Bulk path — COPY FROM STDIN"
    When writing more than 10,000 rows with `mode="append"`, the Rust core automatically switches to PostgreSQL's `COPY FROM STDIN` protocol. The CSV payload is serialized in memory and streamed at wire speed — no temp files. This is typically **10x–50x faster** than multi-row `INSERT`.

---

## 5. MySQL

PardoX uses the `mysql` crate v25.

### Connection string

```
mysql://user:password@host:port/database
```

### Read

```python
from pardox.io import read_mysql

df = read_mysql(
    "mysql://pardox:secret@localhost:3306/mydb",
    "SELECT * FROM products WHERE active = 1"
)
```

### Execute DDL / DML

```python
from pardox.io import execute_mysql

execute_mysql(CONN, "DROP TABLE IF EXISTS products_bak")
execute_mysql(CONN, """
    CREATE TABLE products_bak (
        id       BIGINT,
        name     TEXT,
        price    DOUBLE,
        quantity DOUBLE
    )
""")
```

### Write

```python
# append — chunked INSERT 1,000 rows/stmt (auto LOAD DATA for > 10k if server allows)
rows = df.to_mysql(CONN, "products_bak", mode="append")

# replace — REPLACE INTO (delete + insert)
rows = df.to_mysql(CONN, "products_bak", mode="replace")

# upsert — INSERT … ON DUPLICATE KEY UPDATE
rows = df.to_mysql(CONN, "products_bak", mode="upsert", conflict_cols=["id"])
```

!!! info "LOAD DATA LOCAL INFILE"
    For `append` with more than 10,000 rows, PardoX attempts `LOAD DATA LOCAL INFILE`. If the server has `local_infile=OFF` (MySQL default), it falls back automatically to the 1,000-row chunked INSERT and logs a notice to stderr. To enable the fast path:

    ```sql
    SET GLOBAL local_infile = 1;
    ```

---

## 6. SQL Server

PardoX uses the `tiberius` crate v0.12 with a single-thread Tokio runtime.

### Connection string (ADO.NET format)

```
Server=host,port;Database=db;UID=user;PWD=password;TrustServerCertificate=Yes
```

### Read

```python
from pardox.io import read_sqlserver

CONN = "Server=localhost,1433;Database=mydb;UID=sa;PWD=MyPassword;TrustServerCertificate=Yes"

df = read_sqlserver(CONN, "SELECT TOP 1000 * FROM dbo.orders")
```

### Execute DDL / DML

```python
from pardox.io import execute_sqlserver

execute_sqlserver(CONN, "DROP TABLE IF EXISTS dbo.orders_bak")
execute_sqlserver(CONN, """
    CREATE TABLE dbo.orders_bak (
        id       BIGINT,
        amount   FLOAT,
        region   NVARCHAR(MAX)
    )
""")
```

### Write

```python
# append — multi-row INSERT, 500 rows per statement
rows = df.to_sqlserver(CONN, "dbo.orders_bak", mode="append")

# upsert — MERGE INTO … WHEN MATCHED … WHEN NOT MATCHED
rows = df.to_sqlserver(CONN, "dbo.orders_bak", mode="upsert", conflict_cols=["id"])
```

!!! warning "Password special characters"
    Avoid using `!` in SQL Server passwords. A known issue in tiberius v0.12 causes authentication failure when `!` is present in the password when connecting via TCP from an external host. Use only `[A-Za-z0-9_\-@#$]`. A fix is tracked for v0.3.2.

!!! info "Bulk performance"
    SQL Server writes use 500-row multi-value `INSERT` statements. For 50,000 rows this results in 100 round-trips instead of 50,000. A `BULK INSERT` / `bcp` path is planned for v0.3.2.

---

## 7. MongoDB

PardoX uses the `mongodb` crate v2.8.

### Connection string (MongoDB URI)

```
mongodb://user:password@host:port
```

### Read

```python
from pardox.io import read_mongodb

# Target format: "database.collection"
df = read_mongodb("mongodb://admin:secret@localhost:27017", "mydb.orders")
```

### Execute commands

```python
from pardox.io import execute_mongodb

# Drop a collection
execute_mongodb("mongodb://...", "mydb", '{"drop": "orders_bak"}')
```

### Write

```python
# append — insert_many in batches of 10,000 documents, ordered:false
rows = df.to_mongodb("mongodb://...", "mydb.orders_bak", mode="append")

# replace — drops collection, then inserts all documents
rows = df.to_mongodb("mongodb://...", "mydb.orders_bak", mode="replace")
```

!!! success "Batch behavior"
    `ordered: false` is used on every `insert_many` call. This means MongoDB continues inserting valid documents even if individual documents fail (e.g., duplicate key), rather than aborting the entire batch.

---

## Write Modes Summary

| Database | `append` | `replace` | `upsert` |
|----------|----------|-----------|----------|
| PostgreSQL | INSERT (COPY for >10k) | — | INSERT ON CONFLICT DO UPDATE |
| MySQL | INSERT 1k/stmt (LOAD DATA for >10k) | REPLACE INTO | INSERT ON DUPLICATE KEY UPDATE |
| SQL Server | Multi-row INSERT 500/stmt | Multi-row INSERT 500/stmt | MERGE INTO |
| MongoDB | insert_many 10k/batch | drop + insert_many | — |

---

## Table Must Exist Before Writing

PardoX write methods append to an existing table and do not auto-create it. Always call `execute_*` first:

```python
from pardox.io import execute_sql

execute_sql(CONN, """
    CREATE TABLE IF NOT EXISTS my_table (
        id      BIGINT,
        amount  DOUBLE PRECISION,
        label   TEXT
    )
""")

rows = df.to_sql(CONN, "my_table", mode="append")
```
