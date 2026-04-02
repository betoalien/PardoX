---
layout: default
title: "Quick Start"
parent: "Getting Started"
nav_order: 2
---

# Quick Start

This guide walks through a complete v0.3.4 data pipeline: loading a CSV, running vectorized math, inspecting results, and writing to a relational database — all in native Rust speed.

---

## Hello World

```python
import pardox as px

# 1. Load data — parallel Rust CSV parser, automatic type inference
df = px.read_csv("sales_data.csv")
print(f"Loaded {df.shape[0]:,} rows × {df.shape[1]} columns")

# 2. Inspect columns
print(df.columns)
df.show(5)   # ASCII table preview

# 3. Cast and compute
df.cast("quantity", "Float64")
revenue_df = df.mul("price", "quantity")   # new DataFrame with 'result_mul'

# 4. Statistics
std_val = revenue_df.std("result_mul")
print(f"Revenue std dev: {std_val:,.2f}")

# 5. Write to PostgreSQL (COPY FROM STDIN auto-activated for > 10k rows)
from pardox.io import execute_sql
CONN = "postgresql://user:password@localhost:5432/mydb"
execute_sql(CONN, "CREATE TABLE IF NOT EXISTS sales (price FLOAT, quantity FLOAT)")
rows = df.to_sql(CONN, "sales", mode="append")
print(f"Written {rows:,} rows to PostgreSQL")
```

---

## Step-by-Step Breakdown

### 1. Ingestion (`read_csv`)

PardoX spawns a Rust thread pool to parse chunks of the CSV file in parallel. No Python objects are created during ingestion — data flows directly into Rust-managed memory buffers.

```python
df = px.read_csv("dataset.csv")
```

!!! tip "Type inference"
    The engine scans the first rows to classify each column as `Int64`, `Float64`, or `Utf8` (string). You can override with an explicit schema:

    ```python
    df = px.read_csv("data.csv", schema={"price": "Float64", "id": "Int64"})
    ```

---

### 2. Inspect (`show`, `shape`, `columns`, `dtypes`)

```python
print(df.shape)      # (50000, 14)
print(df.columns)    # ['id', 'price', 'quantity', ...]
print(df.dtypes)     # {'id': 'Utf8', 'price': 'Float64', ...}
df.show(5)           # ASCII table, first 5 rows
```

---

### 3. Type Casting (`cast`)

When a column is inferred as `Int64` but you need `Float64` for arithmetic:

```python
df.cast("quantity", "Float64")
```

Supported types: `Int64`, `Float64`, `Utf8`.

---

### 4. Vectorized Arithmetic

```python
# Series operators (via Proxy / __getitem__)
df['total'] = df['price'] * df['quantity']
df['tax']   = df['total'] * 0.16

# DataFrame-level methods (return new DataFrame)
revenue_df    = df.mul("price", "quantity")   # result column: 'result_mul'
profit_df     = df.sub("revenue", "cost")     # result column: 'result_sub'
```

---

### 5. Native Math Methods

```python
# Standard deviation (pure Rust, no NumPy)
std_val = revenue_df.std("result_mul")

# Min-Max normalization → new DataFrame with 'result_minmax' column
normed_df = df.min_max_scale("price")

# Sort by column (CPU or GPU)
sorted_df = df.sort_values("price", ascending=False)
sorted_df = df.sort_values("price", ascending=True, gpu=True)  # GPU Bitonic sort
```

---

### 6. Observer — Data Inspection & Export

```python
# Value frequency table
state_counts = df.value_counts("state")
print(state_counts)   # {'TX': 6345, 'CA': 6301, ...}

# Unique values in a column
unique_cats = df.unique("category")

# Export full DataFrame to Python
records = df.to_dict()   # list of dicts — 50k records
json_str = df.to_json()  # JSON string "[{...}, ...]"
```

---

### 7. Database Write (Relational Conqueror)

```python
from pardox.io import execute_sql, execute_mysql, read_sql

PG_CONN    = "postgresql://pardox:secret@localhost:5432/mydb"
MYSQL_CONN = "mysql://pardox:secret@localhost:3306/mydb"

# PostgreSQL — auto COPY FROM STDIN for > 10k rows
execute_sql(PG_CONN, "CREATE TABLE IF NOT EXISTS sales (price FLOAT, quantity FLOAT)")
rows = df.to_sql(PG_CONN, "sales", mode="append")
print(f"Postgres: {rows:,} rows written")

# MySQL — chunked batch INSERT (auto LOAD DATA if server allows)
execute_mysql(MYSQL_CONN, "CREATE TABLE IF NOT EXISTS sales (price DOUBLE, quantity DOUBLE)")
rows = df.to_mysql(MYSQL_CONN, "sales", mode="append")
print(f"MySQL: {rows:,} rows written")

# Read back from PostgreSQL
df_check = read_sql(PG_CONN, "SELECT COUNT(*) FROM sales")
```

---

### 8. Zero-Copy NumPy Integration

```python
import numpy as np

# Direct pointer from Rust buffer — no data copy
arr = np.array(df["price"])
print(arr.dtype)   # float64
print(arr.mean())  # same value as df["price"].mean()
```

---

### 9. Persist to .prdx

```python
# Save
df.to_prdx("sales_processed.prdx")

# Load (4.6 GB/s read throughput)
df2 = px.read_prdx("sales_processed.prdx")
```

!!! example "Performance benchmark"
    Loading 2 GB of data: CSV ~8s · Parquet ~3s · **PRDX ~0.5s**

---

## Full Pipeline Example

```python
import pardox as px
from pardox.io import execute_sql

CONN = "postgresql://pardox:secret@localhost:5432/analytics"

# Load
df = px.read_csv("sales_50k.csv")
df.cast("quantity", "Float64")

# Transform
df.fillna(0.0)
df['revenue'] = df['price'] * df['quantity']
df['tax']     = df['revenue'] * 0.08

# Analyze
print(f"Total revenue : ${df['revenue'].sum():,.2f}")
print(f"Avg ticket    : ${df['revenue'].mean():,.2f}")
print(f"Std deviation : {df['revenue'].std():,.2f}")

# Inspect
top_states = df.value_counts("state")
print("Top states:", list(top_states.items())[:5])

# Write
execute_sql(CONN, "DROP TABLE IF EXISTS sales_results")
execute_sql(CONN, (
    "CREATE TABLE sales_results "
    "(price FLOAT, quantity FLOAT, revenue FLOAT, tax FLOAT)"
))
rows = df.to_sql(CONN, "sales_results", mode="append")
print(f"\nWrote {rows:,} rows to PostgreSQL")

# Save locally
df.to_prdx("sales_results.prdx")
print("Saved to sales_results.prdx")
```
