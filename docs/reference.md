---
layout: default
title: "API Reference"
nav_order: 4
---

# API Reference

Complete documentation of all classes, functions, and methods in the PardoX Python SDK v0.3.2.

---

## Top-Level Functions (`import pardox as px`)

### `read_csv`

Reads a CSV file into a DataFrame using the multi-threaded Rust parser.

```python
def read_csv(path: str, schema: dict | None = None) -> DataFrame
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | `str` | Path to the `.csv` file. |
| `schema` | `dict` or `None` | Optional column type overrides: `{"col": "Float64", ...}`. Supported types: `Int64`, `Float64`, `Utf8`. |

**Returns:** `DataFrame`

```python
df = px.read_csv("sales.csv")
df = px.read_csv("sales.csv", schema={"price": "Float64", "id": "Int64"})
```

---

### `read_prdx`

Loads a native PardoX binary file (`.prdx`).

```python
def read_prdx(path: str) -> list[dict]
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | `str` | Path to the `.prdx` file. |

**Returns:** `list[dict]` (preview rows)

---

### `from_arrow`

Zero-copy conversion from a PyArrow Table or RecordBatch.

```python
def from_arrow(data: pyarrow.Table | pyarrow.RecordBatch) -> DataFrame
```

```python
import pyarrow as pa, pardox as px
arrow_table = pa.Table.from_pydict({"a": [1, 2, 3]})
df = px.from_arrow(arrow_table)
```

---

## `pardox.io` — Database I/O

All database functions bypass the Python runtime — connection and data transfer happen entirely in the Rust core.

### PostgreSQL

#### `read_sql(connection_string, query) → DataFrame`

```python
from pardox.io import read_sql
df = read_sql("postgresql://user:pass@localhost:5432/db", "SELECT * FROM orders")
```

#### `execute_sql(connection_string, query) → int`

Executes DDL or DML. Returns rows affected (0 for DDL).

```python
from pardox.io import execute_sql
execute_sql(CONN, "DROP TABLE IF EXISTS orders")
execute_sql(CONN, "CREATE TABLE orders (id BIGINT, amount FLOAT)")
n = execute_sql(CONN, "DELETE FROM orders WHERE status = 'cancelled'")
```

**Raises:** `RuntimeError` on connection or SQL failure.

---

### MySQL

#### `read_mysql(connection_string, query) → DataFrame`

```python
from pardox.io import read_mysql
df = read_mysql("mysql://user:pass@localhost:3306/db", "SELECT * FROM products")
```

#### `execute_mysql(connection_string, query) → int`

```python
from pardox.io import execute_mysql
execute_mysql(CONN, "CREATE TABLE IF NOT EXISTS products (id BIGINT, price DOUBLE)")
```

---

### SQL Server

#### `read_sqlserver(connection_string, query) → DataFrame`

```python
from pardox.io import read_sqlserver
CONN = "Server=localhost,1433;Database=mydb;UID=sa;PWD=MyPwd;TrustServerCertificate=Yes"
df = read_sqlserver(CONN, "SELECT TOP 1000 * FROM dbo.orders")
```

#### `execute_sqlserver(connection_string, query) → int`

```python
from pardox.io import execute_sqlserver
execute_sqlserver(CONN, "DROP TABLE IF EXISTS dbo.orders_bak")
```

!!! warning "Password special characters"
    Avoid `!` in SQL Server passwords. Known tiberius v0.12 bug — fix planned for v0.3.2.

---

### MongoDB

#### `read_mongodb(connection_string, db_dot_collection) → DataFrame`

```python
from pardox.io import read_mongodb
df = read_mongodb("mongodb://admin:pass@localhost:27017", "mydb.orders")
```

#### `execute_mongodb(connection_string, database, command_json) → int`

```python
from pardox.io import execute_mongodb
execute_mongodb("mongodb://...", "mydb", '{"drop": "orders_archive"}')
```

---

## Class: `DataFrame`

The main data structure. Holds an opaque pointer to a Rust `HyperBlockManager`.

### Construction

```python
# From CSV
df = px.read_csv("file.csv")

# From SQL
df = read_sql(conn, "SELECT …")

# From MySQL / SQL Server / MongoDB
df = read_mysql(conn, query)
df = read_sqlserver(conn, query)
df = read_mongodb(conn, "db.collection")

# From Arrow
df = px.from_arrow(arrow_table)
```

### Properties

#### `shape → tuple[int, int]`

```python
rows, cols = df.shape
print(f"{rows:,} rows × {cols} columns")
```

#### `columns → list[str]`

```python
print(df.columns)   # ['id', 'price', 'quantity', ...]
```

#### `dtypes → dict[str, str]`

```python
print(df.dtypes)    # {'id': 'Utf8', 'price': 'Float64', 'quantity': 'Int64'}
```

---

### Inspection

#### `show(n=10)`

Prints the first `n` rows as an ASCII table to stdout.

```python
df.show(5)
```

#### `head(n=5) → DataFrame`

Returns a new DataFrame with the first `n` rows.

```python
top5 = df.head(5)
```

#### `tail(n=5) → DataFrame`

Returns a new DataFrame with the last `n` rows.

```python
last5 = df.tail(5)
```

#### `iloc(start, end) → DataFrame`

Returns rows in the range `[start, end)`.

```python
subset = df.iloc(100, 200)   # rows 100–199
```

---

### Type Operations

#### `cast(col, target_type) → DataFrame`

Converts a column to a new type in-place. Returns `self`.

```python
df.cast("quantity", "Float64")
df.cast("id",       "Utf8")
```

**Supported types:** `Int64`, `Float64`, `Utf8`

---

### Arithmetic Methods

All arithmetic methods return a **new DataFrame** with the result stored in a named column.

#### `mul(col_a, col_b) → DataFrame`

```python
revenue_df = df.mul("price", "quantity")   # result column: 'result_mul'
```

#### `add(col_a, col_b) → DataFrame`

```python
total_df = df.add("price", "tax")          # result column: 'result_add'
```

#### `sub(col_a, col_b) → DataFrame`

```python
profit_df = df.sub("revenue", "cost")      # result column: 'result_sub'
```

#### `std(col) → float`

Sample standard deviation of a column. Pure Rust, no NumPy.

```python
std_val = revenue_df.std("result_mul")
```

#### `min_max_scale(col) → DataFrame`

Normalizes column values to `[0, 1]`. Returns new DataFrame with `result_minmax`.

```python
normed_df = df.min_max_scale("price")
```

---

### Sorting

#### `sort_values(by, ascending=True, gpu=False) → DataFrame`

Sorts the DataFrame by a `Float64` column. Returns a **new sorted DataFrame**.

```python
sorted_df = df.sort_values("price", ascending=True)
sorted_df = df.sort_values("price", ascending=False, gpu=True)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `by` | `str` | Column name to sort by. Must be `Float64`. |
| `ascending` | `bool` | `True` = ascending (default). |
| `gpu` | `bool` | Use GPU Bitonic sort. Falls back to CPU if GPU unavailable. |

---

### Filtering

#### `filter(mask: Series) → DataFrame`

Applies a boolean Series as a row filter. Returns a new DataFrame.

```python
mask   = df['price'] > 100.0
result = df.filter(mask)
```

---

### Data Cleaning

#### `fillna(value: float) → DataFrame`

Fills `NaN` / null values in all numeric columns in-place.

```python
df.fillna(0.0)
```

#### `round(decimals: int) → DataFrame`

Rounds all numeric columns in-place.

```python
df.round(2)
```

---

### Observer — Export & Inspection

#### `to_dict() → list[dict]`

Returns all rows as a list of dictionaries (records format).

```python
records = df.to_dict()
# [{'price': 19.99, 'state': 'TX', ...}, ...]
```

**Returns:** `list[dict]`

#### `to_json() → str`

Returns all rows as a JSON string `"[{...}, ...]"`.

```python
json_str = df.to_json()
```

**Returns:** `str`

#### `value_counts(col) → dict[str, int]`

Frequency of each unique value in a column, sorted by count descending.

```python
state_dist = df.value_counts("state")
# {'TX': 6345, 'CA': 6301, ...}
```

**Returns:** `dict[str, int]`

#### `unique(col) → list`

Unique values in a column in insertion order.

```python
cats = df.unique("category")
# ['Electronics', 'Books', ...]
```

**Returns:** `list`

---

### Joins

#### `join(other, on=None, left_on=None, right_on=None) → DataFrame`

Hash-join two DataFrames on a key column.

```python
result = orders.join(customers, on="customer_id")
result = orders.join(customers, left_on="cust_id", right_on="id")
```

---

### Writers

#### `to_prdx(path) → bool`

Saves DataFrame to native binary format.

```python
df.to_prdx("output.prdx")
```

#### `to_csv(path) → bool`

Exports DataFrame to a CSV file.

```python
df.to_csv("output.csv")
```

#### `to_sql(connection_string, table_name, mode="append", conflict_cols=[]) → int`

Writes to PostgreSQL.

```python
rows = df.to_sql(CONN, "orders", mode="append")
rows = df.to_sql(CONN, "orders", mode="upsert", conflict_cols=["id"])
```

| Parameter | Type | Values |
|-----------|------|--------|
| `mode` | `str` | `"append"`, `"upsert"` |
| `conflict_cols` | `list[str]` | Columns for `ON CONFLICT` clause (upsert only) |

**Returns:** `int` — rows written. **Raises:** `RuntimeError` on failure.

---

### `write_sql_prdx` — PRDX Streaming to PostgreSQL

*Added in v0.3.2*

Stream a `.prdx` file directly to PostgreSQL via `COPY FROM STDIN` — **O(block) RAM** regardless of file size. The schema is read from the PRDX footer; data is never fully loaded into memory.

```python
from pardox import write_sql_prdx

rows = write_sql_prdx(
    prdx_path,        # str — path to .prdx file
    connection_string, # str — PostgreSQL connection string
    table_name,        # str — target table (must already exist)
    mode="append",     # str — only "append" supported
    conflict_cols=[],  # list[str] — reserved for future upsert support
    batch_rows=1000000 # int — rows per COPY batch
)
print(f"Streamed {rows:,} rows")
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `prdx_path` | `str` | Path to the `.prdx` file |
| `connection_string` | `str` | PostgreSQL connection string (`postgresql://user:pass@host:port/db`) |
| `table_name` | `str` | Target table name (must exist with matching schema) |
| `mode` | `str` | Write mode — only `"append"` supported in v0.3.2 |
| `conflict_cols` | `list[str]` | Reserved — pass `[]` |
| `batch_rows` | `int` | Rows per COPY batch (default: 1,000,000) |

**Returns:** `int` — total rows written. **Raises:** `RuntimeError` on failure.

**Validated:** 150M rows / 3.8 GB PRDX → PostgreSQL in ~490s at ~300,000 rows/s.

#### `to_mysql(connection_string, table_name, mode="append", conflict_cols=[]) → int`

Writes to MySQL.

```python
rows = df.to_mysql(CONN, "products", mode="append")
rows = df.to_mysql(CONN, "products", mode="replace")
rows = df.to_mysql(CONN, "products", mode="upsert", conflict_cols=["id"])
```

| Parameter | Type | Values |
|-----------|------|--------|
| `mode` | `str` | `"append"`, `"replace"`, `"upsert"` |

#### `to_sqlserver(connection_string, table_name, mode="append", conflict_cols=[]) → int`

Writes to SQL Server (batch INSERT 500 rows/stmt).

```python
rows = df.to_sqlserver(CONN, "dbo.orders", mode="append")
rows = df.to_sqlserver(CONN, "dbo.orders", mode="upsert", conflict_cols=["id"])
```

| Parameter | Type | Values |
|-----------|------|--------|
| `mode` | `str` | `"append"`, `"replace"`, `"upsert"` |

#### `to_mongodb(connection_string, db_dot_collection, mode="append") → int`

Writes to MongoDB (10,000 docs/batch, `ordered: false`).

```python
rows = df.to_mongodb(CONN, "mydb.orders", mode="append")
rows = df.to_mongodb(CONN, "mydb.orders", mode="replace")
```

| Parameter | Type | Values |
|-----------|------|--------|
| `mode` | `str` | `"append"`, `"replace"` |

---

## Class: `Series`

A single column view into a DataFrame. Returned by `df['col_name']`. Does **not** own the underlying memory — the parent DataFrame does.

### Properties

#### `name → str`

Column name.

#### `dtype → str`

Column type (`"Int64"`, `"Float64"`, `"Utf8"`).

---

### Arithmetic Operators

Operations dispatch to SIMD-accelerated Rust kernels. All return a new `Series`.

```python
total = df['price'] * df['quantity']
net   = df['total'] - df['discount']
tax   = df['total'] + df['tax_amount']
unit  = df['revenue'] / df['quantity']
```

---

### Comparison Operators

Return a boolean `Series` usable as a filter mask.

| Method | Meaning |
|--------|---------|
| `s.eq(val)` | `==` |
| `s.neq(val)` | `!=` |
| `s.gt(val)` | `>` |
| `s.gte(val)` | `>=` |
| `s.lt(val)` | `<` |
| `s.lte(val)` | `<=` |

```python
mask = df['price'].gt(100.0)
df_filtered = df.filter(mask)

mask2 = df['state'].eq("TX")
df_tx = df.filter(mask2)
```

---

### Aggregations

All aggregation methods return a Python scalar.

| Method | Returns | Description |
|--------|---------|-------------|
| `sum()` | `float` | Sum of all non-null values |
| `mean()` | `float` | Arithmetic mean |
| `min()` | `float` | Minimum value |
| `max()` | `float` | Maximum value |
| `std()` | `float` | Sample standard deviation |
| `count()` | `int` | Count of non-null values |

```python
total   = df['revenue'].sum()
average = df['revenue'].mean()
high    = df['revenue'].max()
low     = df['revenue'].min()
spread  = df['revenue'].std()
valid   = df['id'].count()
```

---

### Transformations

#### `fillna(value) → Series`

```python
df['price'].fillna(0.0)
```

#### `round(decimals) → Series`

```python
df['price'].round(2)
```

---

### NumPy Zero-Copy

```python
import numpy as np

# Direct pointer into Rust buffer — no allocation
arr = np.array(df['price'])   # dtype: float64
```

Works on `Float64` columns. Cast `Int64` columns first:

```python
df.cast("quantity", "Float64")
arr = np.array(df["quantity"])
```

---

## Error Codes

All database functions raise `RuntimeError` with a descriptive message on failure. The underlying Rust function returns integer error codes:

| Code | Meaning |
|------|---------|
| `-1` | Invalid manager pointer (null) |
| `-2` | Invalid connection string |
| `-3` | Invalid table / query string |
| `-4` | Invalid mode string |
| `-5` | Invalid conflict columns JSON |
| `-10` | File not found (`write_sql_prdx` only) |
| `-20` | Empty connection string (`write_sql_prdx` only) |
| `-100` | Operation failed — check stderr for Rust error details |

!!! tip "Stderr logging"
    When `-100` is returned, the Rust core logs the actual database error to stderr before returning. Run with stderr visible to diagnose connection or schema issues.
