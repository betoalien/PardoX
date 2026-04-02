# PardoX — High-Performance DataFrame Engine

[![PyPI version](https://badge.fury.io/py/pardox.svg)](https://badge.fury.io/py/pardox)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Powered By Rust](https://img.shields.io/badge/powered%20by-Rust-orange.svg)](https://www.rust-lang.org/)

**The Speed of Rust. The Simplicity of Python.**

PardoX is a next-generation DataFrame engine for high-performance ETL, data analysis, and database integration. A single **Rust core** powers the entire computation layer — Python is just the interface.

> **v0.3.4 is now available.** PRDX streaming to PostgreSQL (150M rows validated), GroupBy, String & Date ops, Window functions, Lazy pipeline, SQL over DataFrames, Encryption, Data Contracts, Time Travel, Arrow Flight, Distributed Cluster, Linear Algebra, REST Connector, Cloud Storage — 29 gap features total.

---

## ⚡ Why PardoX?

| Capability | How |
|------------|-----|
| **Zero-copy ingestion** | Multi-threaded Rust CSV parser, data flows directly into HyperBlock buffers |
| **SIMD arithmetic** | AVX2 / NEON instructions — 5x–20x faster than Python loops |
| **Native database I/O** | Connect to PostgreSQL, MySQL, SQL Server, MongoDB — no `psycopg2`, no `pymysql`, no ORM |
| **PRDX Streaming** | Stream 150M-row files to PostgreSQL at ~306k rows/s with O(block) RAM |
| **GPU sort** | WebGPU Bitonic sort with transparent CPU fallback |
| **GroupBy + Window** | Aggregations, rolling, rank, lag/lead — pure Rust |
| **NumPy bridge** | Zero-copy `np.array(df['col'])` — direct pointer into Rust buffer |
| **Zero dependencies** | Pure ctypes — no pip dependencies required |
| **Cross-platform** | Linux x64 · Windows x64 · macOS Intel · macOS Apple Silicon |

---

## 📦 Installation

```bash
pip install pardox
```

No Rust compiler. No C extensions to build. No database drivers to install.

**Requirements:** Python 3.8+

---

## 🚀 Quick Start

```python
import pardox as px
from pardox.io import execute_sql

# Load 100,000 rows — parallel Rust CSV parser
df = px.read_csv("sales_data.csv")
print(f"Loaded {df.shape[0]:,} rows × {df.shape[1]} columns")

# SIMD-accelerated arithmetic
df.cast("quantity", "Float64")
df['revenue'] = df['price'] * df['quantity']

# Statistics — pure Rust, no NumPy needed
print(f"Total revenue : ${df['revenue'].sum():,.2f}")
print(f"Avg ticket    : ${df['revenue'].mean():,.2f}")

# GroupBy aggregation
grouped = df.groupby("state", {"revenue": "sum", "quantity": "mean"})

# Write to PostgreSQL — COPY FROM STDIN auto-activated for > 10k rows
CONN = "postgresql://user:password@localhost:5432/mydb"
execute_sql(CONN, "CREATE TABLE IF NOT EXISTS sales (price FLOAT, quantity FLOAT, revenue FLOAT)")
rows = df.to_sql(CONN, "sales", mode="append")
print(f"Written {rows:,} rows to PostgreSQL")

# Save locally — 4.6 GB/s read throughput
df.to_prdx("sales_processed.prdx")
```

---

## 🗄️ What's New in v0.3.4

### PRDX Streaming to PostgreSQL

Stream a `.prdx` file directly to PostgreSQL — **without loading the file into RAM**. O(block) memory regardless of file size.

```python
from pardox import write_sql_prdx

rows = write_sql_prdx(
    "ventas_150m.prdx",                         # .prdx file
    "postgresql://user:pass@localhost:5432/db",  # connection string
    "ventas",                                   # table (must already exist)
    mode="append",
    conflict_cols=[],
    batch_rows=1_000_000
)
print(f"Streamed {rows:,} rows")
# Validated: 150,000,000 rows / 3.8 GB in ~490s at ~306k rows/s
```

| Approach | RAM used |
|----------|----------|
| `px.read_prdx()` then `df.to_sql()` | Entire file (3.8 GB for 150M rows) |
| `write_sql_prdx()` | O(one block) — typically < 200 MB |

---

### GroupBy Aggregation

```python
# Single aggregation
grouped = df.groupby("category", {"revenue": "sum"})

# Multiple aggregations
grouped = df.groupby("state", {
    "revenue":  "sum",
    "price":    "mean",
    "quantity": "count",
})
```

---

### String & Date Operations

```python
# String ops
df.str_upper("name")
df.str_lower("email")
df.str_trim("description")
df.str_contains("tag", "python")
df.str_replace("status", "old", "new")

# Date ops
df.date_extract("created_at", "year")   # → 'result_year'
df.date_format("created_at", "%Y-%m")
df.date_diff("end_date", "start_date")
df.date_add("created_at", 30, "day")
```

---

### Window Functions

```python
df.row_number("price")
df.rank("revenue", method="dense")
df.lag("price", 1)
df.lead("price", 1)
df.rolling_mean("price", 7)    # 7-period moving average
```

---

### Lazy Pipeline

```python
import pardox as px

# Scan without loading — filter and collect on demand
result = (
    px.scan_csv("large_file.csv")
    .select(["id", "price", "state"])
    .filter("price", ">", 100.0)
    .limit(10_000)
    .collect()
)
print(f"{result.shape[0]} rows")
```

---

### SQL over DataFrames

```python
# Run SQL directly on a DataFrame in memory
result = df.sql("SELECT state, SUM(revenue) as total FROM df GROUP BY state")
```

---

### Out-of-Core Processing (Large Datasets > RAM)

Stream a SQL table to a `.prdx` file with O(batch) RAM, then run analytics on it
without ever loading the full dataset — ideal for 100M+ row workloads.

```python
import pardox as px

# Step 1: Stream SQL table to disk (O(batch) RAM — ~200 MB regardless of table size)
rows = px.write_sql_prdx(
    "orders.prdx",
    "postgresql://user:pass@localhost:5432/db",
    "orders",
    mode="append",
    conflict_cols=[],
    batch_rows=1_000_000
)
print(f"Saved {rows:,} rows")   # validated: 150M rows / ~490s / ~306k rows/s

# Step 2: GroupBy directly on .prdx — memory = O(distinct groups), not O(rows)
result = px.prdx_groupby(
    "orders.prdx",
    ["product_id", "region"],
    {"amount": "sum", "qty": "count"}
)
result.show(20)

# Aggregate statistics without loading the file
total  = px.prdx_count("orders.prdx")
avg    = px.prdx_mean("orders.prdx",  "amount")
top    = px.prdx_max("orders.prdx",   "amount")
bottom = px.prdx_min("orders.prdx",   "amount")
```

In-memory chunked processing (when data is already loaded):

```python
# GroupBy in configurable chunks — peak RAM = chunk_size × row_bytes
result = df.chunked_groupby(
    "product_id",
    {"amount": "sum", "qty": "count"},
    chunk_size=1_000_000
)

# External merge sort — handles datasets larger than RAM
sorted_df = df.external_sort("amount", ascending=False, chunk_size=1_000_000)

# Spill manager to disk and restore
df.spill_to_disk("/tmp/orders_spill")
df2 = px.DataFrame.spill_from_disk("/tmp/orders_spill")

# Current memory usage
print(f"RSS: {px.DataFrame.memory_usage() / 1e6:.1f} MB")
```

| Approach | RAM used |
|----------|----------|
| `read_sql()` on 100M rows | ~2–4 GB (full result in memory) |
| `write_sql_prdx()` + `prdx_groupby()` | O(batch) write + O(groups) query |
| `chunked_groupby()` | O(chunk_size × row_bytes) |

---

### Encryption

```python
import pardox as px

# Write encrypted PRDX
px.write_prdx_encrypted("secure.prdx", df, "my-secret-key")

# Read back
df = px.read_prdx_encrypted("secure.prdx", "my-secret-key")
```

---

### Data Contracts

```python
import json

contract = json.dumps({
    "source": "orders",
    "columns": {
        "price":    {"min": 0.0, "max": 10000.0},
        "status":   {"allowed_values": ["active", "pending", "closed"]},
        "customer": {"nullable": False},
    }
})

# Returns a new DataFrame with only conforming rows
clean = df.validate_contract(contract)
violations = df.contract_violation_count()
print(f"{violations} rows quarantined")
```

---

### Time Travel

```python
import pardox as px

# Save a versioned snapshot
px.version_write(df, "/data/snapshots", "v1", timestamp=0)

# Restore a snapshot by label
df_v1 = px.version_read("/data/snapshots", "v1")

# List available versions
versions = px.version_list("/data/snapshots")
```

---

### Linear Algebra

```python
# Cosine similarity between two DataFrame columns
sim = df.cosine_sim("embeddings", df2, "embeddings")

# L2 normalization
normed = df.l2_normalize("features")

# Matrix multiplication
result = df.matmul("A", df2, "B")

# PCA — reduce to N components
pca_df = df.pca("features", n_components=3)
```

---

### Cloud Storage

```python
import pardox as px

# Read CSV from S3, GCS, or Azure
df = px.read_cloud_csv(
    "s3://my-bucket/data.csv",
    schema={},
    config={},
    credentials={"access_key_id": "...", "secret_access_key": "..."}
)
```

---

### REST Connector

```python
import pardox as px

# Read from a REST endpoint directly into a DataFrame
df = px.read_rest("https://api.example.com/records", "GET", "{}")
```

---

## 🗄️ Database I/O

```python
from pardox.io import (
    read_sql, execute_sql,                    # PostgreSQL
    read_mysql, execute_mysql,                # MySQL
    read_sqlserver, execute_sqlserver,        # SQL Server
    read_mongodb, execute_mongodb,            # MongoDB
)

# ── PostgreSQL ───────────────────────────────────────────────
PG = "postgresql://user:pass@localhost:5432/db"

df = read_sql(PG, "SELECT * FROM orders WHERE status = 'active'")
execute_sql(PG, "CREATE TABLE orders_archive (id BIGINT, amount FLOAT, region TEXT)")
rows = df.to_sql(PG, "orders_archive", mode="append")           # COPY FROM STDIN for > 10k
rows = df.to_sql(PG, "orders_archive", mode="upsert", conflict_cols=["id"])

# ── MySQL ────────────────────────────────────────────────────
MY = "mysql://user:pass@localhost:3306/db"

df = read_mysql(MY, "SELECT * FROM products WHERE active = 1")
execute_mysql(MY, "CREATE TABLE IF NOT EXISTS products_bak (id BIGINT, price DOUBLE)")
rows = df.to_mysql(MY, "products_bak", mode="append")
rows = df.to_mysql(MY, "products_bak", mode="upsert", conflict_cols=["id"])

# ── SQL Server ───────────────────────────────────────────────
MS = "Server=localhost,1433;Database=mydb;UID=sa;PWD=MyPwd;TrustServerCertificate=Yes"

df = read_sqlserver(MS, "SELECT TOP 5000 * FROM dbo.transactions")
rows = df.to_sqlserver(MS, "dbo.transactions_bak", mode="upsert", conflict_cols=["id"])

# ── MongoDB ──────────────────────────────────────────────────
MG = "mongodb://admin:pass@localhost:27017"

df = read_mongodb(MG, "mydb.orders")
rows = df.to_mongodb(MG, "mydb.orders_archive", mode="append")
```

**Write modes:**

| Database | `append` | `replace` | `upsert` |
|----------|----------|-----------|----------|
| PostgreSQL | INSERT (COPY for >10k) | — | ON CONFLICT DO UPDATE |
| MySQL | INSERT 1k/stmt (LOAD DATA for >10k) | REPLACE INTO | ON DUPLICATE KEY UPDATE |
| SQL Server | INSERT 500/stmt | INSERT 500/stmt | MERGE INTO |
| MongoDB | insert_many 10k/batch | drop + insert_many | — |

> **Note on SQL Server passwords:** Avoid using `!` in SQL Server passwords. A known issue in the tiberius v0.12 Rust driver causes authentication failure when `!` is present. Use only `[A-Za-z0-9_\-@#$]`.

---

## 📋 Full API Overview

### Top-level functions

```python
import pardox as px

df = px.read_csv("file.csv", schema={"price": "Float64"})
df = px.read_prdx("file.prdx")
df = px.from_arrow(arrow_table)            # zero-copy from PyArrow
df = px.scan_csv("file.csv").collect()     # lazy load
df = px.read_cloud_csv(url, schema, config, credentials)
df = px.read_rest(url, method, headers_json)
df = px.read_prdx_encrypted("file.prdx", "key")

rows = px.write_sql_prdx(path, conn, table, mode, conflict_cols, batch_rows)
px.write_prdx_encrypted("file.prdx", df, "key")

df = px.version_read(path, label)
labels = px.version_list(path)
px.version_write(df, path, label)

# Out-of-core / streaming PRDX
result  = px.prdx_groupby("file.prdx", ["col1"], {"col2": "sum"})
total   = px.prdx_count("file.prdx")
avg     = px.prdx_mean("file.prdx", "col")
maximum = px.prdx_max("file.prdx", "col")
minimum = px.prdx_min("file.prdx", "col")
```

### DataFrame — Properties & Inspection

```python
df.shape          # (rows, cols)
df.columns        # ['col1', 'col2', ...]
df.dtypes         # {'col1': 'Float64', ...}
df.show(10)       # ASCII table preview
df.head(5)        # → DataFrame
df.tail(5)        # → DataFrame
df.iloc(0, 100)   # → DataFrame (rows 0-99)
```

### DataFrame — Arithmetic & Transform

```python
df['revenue'] = df['price'] * df['quantity']   # Series operators
df.cast("col", "Float64")
df.fillna(0.0)
df.round(2)
df.mul("price", "quantity")       # → DataFrame with 'result_mul'
df.sub("revenue", "cost")         # → DataFrame with 'result_sub'
df.min_max_scale("price")         # → DataFrame with 'result_minmax'
df.std("price")                   # float
df.sort_values("price", ascending=True, gpu=False)
```

### DataFrame — Out-of-Core

```python
df.chunked_groupby("col", {"val": "sum"}, chunk_size=1_000_000)
df.external_sort("col", ascending=True, chunk_size=1_000_000)
df.spill_to_disk("/tmp/spill_path")
px.DataFrame.spill_from_disk("/tmp/spill_path")    # → DataFrame
px.DataFrame.memory_usage()                         # → bytes (RSS)
```

### DataFrame — GroupBy & Aggregation

```python
df.groupby("category", {"revenue": "sum", "price": "mean"})
df.groupby("state", {"quantity": "count", "revenue": "max"})
```

### DataFrame — Window Functions

```python
df.row_number("price")
df.rank("revenue", method="dense")
df.lag("price", 1)
df.lead("price", 1)
df.rolling_mean("price", 7)
```

### DataFrame — String & Date

```python
df.str_upper("col")
df.str_lower("col")
df.str_trim("col")
df.str_contains("col", "pattern")
df.str_replace("col", "old", "new")

df.date_extract("col", "year")
df.date_format("col", "%Y-%m-%d")
df.date_diff("end", "start")
df.date_add("col", 30, "day")
```

### DataFrame — Filtering & Join

```python
mask = df['price'].gt(100.0)
df_filtered = df.filter(mask)

result = df.join(df2, on="customer_id")
result = df.join(df2, left_on="cust_id", right_on="id")
```

### Series — Aggregations

```python
df['col'].sum()    # float
df['col'].mean()   # float
df['col'].min()    # float
df['col'].max()    # float
df['col'].std()    # float
df['col'].count()  # int
```

### Observer

```python
df.value_counts("col")   # dict[str, int]
df.unique("col")         # list
df.to_dict()             # list[dict]
df.to_json()             # str
```

### Write

```python
df.to_prdx("out.prdx")
df.to_csv("out.csv")
df.to_sql(conn, "table", mode="append", conflict_cols=[])
df.to_mysql(conn, "table", mode="upsert", conflict_cols=["id"])
df.to_sqlserver(conn, "dbo.table", mode="append")
df.to_mongodb(conn, "db.collection", mode="append")
px.write_sql_prdx("file.prdx", conn, "table", mode="append", conflict_cols=[], batch_rows=1_000_000)
```

### NumPy Zero-Copy Bridge

```python
import numpy as np

arr = np.array(df["price"])   # dtype: float64 — direct pointer into Rust buffer

# Compatible with Scikit-Learn out of the box
from sklearn.linear_model import LinearRegression
X = np.column_stack([np.array(df["price"]), np.array(df["quantity"])])
y = np.array(df["revenue"])
model = LinearRegression().fit(X, y)
```

---

## 📊 Benchmarks

Hardware: MacBook Pro M2, 16 GB RAM.

| Operation | Pandas v2.x | PardoX v0.3.4 | Speedup |
|-----------|------------|---------------|---------|
| Read CSV (1 GB) | 4.2s | 0.8s | **5.2x** |
| Column multiply | 0.15s | 0.02s | **7.5x** |
| Fill NA | 0.30s | 0.04s | **7.5x** |
| Read binary | 0.9s (Parquet) | 0.2s (.prdx) | **4.5x** |
| PostgreSQL write 50k rows | ~18s (psycopg2) | ~0.6s (COPY) | **~30x** |
| MySQL write 50k rows | ~22s (pymysql) | ~3s (batch INSERT) | **~7x** |
| PRDX → PostgreSQL 150M rows | N/A | ~490s | 306k rows/s |

---

## 🗺️ Roadmap

| Version | Status | Highlights |
|---------|--------|------------|
| v0.3.4 | ✅ Current | PRDX Streaming, GroupBy, Window, String/Date, Lazy, SQL over DF, Encryption, Data Contracts, Time Travel, Arrow Flight, Distributed Cluster, Linear Algebra, REST Connector, Cloud Storage, Out-of-Core Processing — 29 features |

---

## 🌐 Platform Support

| OS | Architecture | Status |
|----|-------------|--------|
| Linux | x86_64 | ✅ Stable |
| Windows | x86_64 | ✅ Stable |
| macOS | ARM64 (M1/M2/M3) | ✅ Stable |
| macOS | x86_64 (Intel) | ✅ Stable |

---

## 📘 Documentation

[**Full Documentation →**](https://www.pardox.io)

---

## 📄 License

MIT License — free for commercial and personal use.

---

<p align="center">by Alberto Cardenas<br>
<a href="https://www.albertocardenas.com">www.albertocardenas.com</a> · <a href="https://www.pardox.io">www.pardox.io</a></p>
