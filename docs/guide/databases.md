---
layout: default
title: "Database Integration"
parent: "User Guide"
nav_order: 2
---

# Database Integration

PardoX v0.3.2 provides native database connectivity via Rust drivers — no Python, PHP, or Node.js database libraries required. The same Rust core powers identical APIs across all three SDKs.

---

## Architecture

```
Python / Node.js / PHP
        │
        │  connection string + DataFrame pointer
        ▼
   Rust Core (libpardox.so)
        │
        ├── tokio-postgres  → PostgreSQL
        ├── mysql v25       → MySQL / MariaDB
        ├── tiberius v0.12  → SQL Server
        └── mongodb v2.8    → MongoDB
```

The host language sends a connection string and a raw pointer to the HyperBlock. Rust manages the entire wire protocol, serialization, and bulk transfer. No ORM, no query builder, no intermediate Python objects.

---

## Supported Databases

| Database | Rust driver | Read | Write | Execute DDL/DML |
|----------|-------------|------|-------|-----------------|
| PostgreSQL | `tokio-postgres` | ✅ | ✅ | ✅ |
| MySQL / MariaDB | `mysql` v25 | ✅ | ✅ | ✅ |
| SQL Server | `tiberius` v0.12 | ✅ | ✅ | ✅ |
| MongoDB | `mongodb` v2.8 | ✅ | ✅ | ✅ |

---

## Connection String Formats

| Database | Format |
|----------|--------|
| PostgreSQL | `postgresql://user:password@host:port/database` |
| MySQL | `mysql://user:password@host:port/database` |
| SQL Server | `Server=host,port;Database=db;UID=user;PWD=password;TrustServerCertificate=Yes` |
| MongoDB | `mongodb://user:password@host:port` |

---

## PostgreSQL

### Python

```python
from pardox.io import read_sql, execute_sql
import pardox as px

CONN = "postgresql://pardox:secret@localhost:5432/mydb"

# Read
df = read_sql(CONN, "SELECT * FROM orders WHERE status = 'active'")
print(df.shape)   # (rows, cols)

# DDL
execute_sql(CONN, "DROP TABLE IF EXISTS orders_processed")
execute_sql(CONN, """
    CREATE TABLE orders_processed (
        id      BIGINT,
        amount  DOUBLE PRECISION,
        region  TEXT,
        UNIQUE (id)
    )
""")

# Write — append (auto COPY FROM STDIN for > 10k rows)
rows = df.to_sql(CONN, "orders_processed", mode="append")
print(f"Written {rows:,} rows")

# Write — upsert
rows = df.to_sql(CONN, "orders_processed", mode="upsert", conflict_cols=["id"])
```

### Node.js

```js
const { read_sql, executeSql } = require('pardox/src/io');

const CONN = 'postgresql://pardox:secret@localhost:5432/mydb';

const df = read_sql(CONN, 'SELECT * FROM orders');
executeSql(CONN, 'CREATE TABLE IF NOT EXISTS orders_bak (id BIGINT, amount FLOAT)');

const rows = df.toSql(CONN, 'orders_bak', 'append');
console.log(`Written ${rows} rows`);

// Upsert
const rows2 = df.toSql(CONN, 'orders_bak', 'upsert', ['id']);
```

### PHP

```php
use PardoX\DataFrame;
use PardoX\IO;

$CONN = 'postgresql://pardox:secret@localhost:5432/mydb';

$df = IO::read_sql($CONN, 'SELECT * FROM orders');
IO::executeSql($CONN, 'CREATE TABLE IF NOT EXISTS orders_bak (id BIGINT, amount FLOAT)');

$rows = $df->to_sql($CONN, 'orders_bak', 'append');
echo "Written $rows rows\n";

// Upsert
$rows = $df->to_sql($CONN, 'orders_bak', 'upsert', ['id']);
```

### Write modes

| Mode | SQL generated |
|------|--------------|
| `append` | `INSERT INTO table (cols) VALUES …` Auto-upgrades to `COPY FROM STDIN` for > 10,000 rows |
| `upsert` | `INSERT INTO table (cols) VALUES … ON CONFLICT (key_cols) DO UPDATE SET …` |

---

## MySQL

### Python

```python
from pardox.io import read_mysql, execute_mysql

CONN = "mysql://pardox:secret@localhost:3306/mydb"

# Read
df = read_mysql(CONN, "SELECT * FROM products WHERE stock > 0")

# DDL
execute_mysql(CONN, "DROP TABLE IF EXISTS products_bak")
execute_mysql(CONN, """
    CREATE TABLE products_bak (
        id       BIGINT PRIMARY KEY,
        name     TEXT,
        price    DOUBLE,
        quantity DOUBLE
    )
""")

# Write
rows = df.to_mysql(CONN, "products_bak", mode="append")
rows = df.to_mysql(CONN, "products_bak", mode="replace")
rows = df.to_mysql(CONN, "products_bak", mode="upsert", conflict_cols=["id"])
```

### Node.js

```js
const { read_mysql, execute_mysql } = require('pardox/src/io');

const df = read_mysql(CONN, 'SELECT * FROM products');
execute_mysql(CONN, 'CREATE TABLE IF NOT EXISTS products_bak (id BIGINT, price DOUBLE)');

const rows = df.toMysql(CONN, 'products_bak', 'append');
const rows2 = df.toMysql(CONN, 'products_bak', 'upsert', ['id']);
```

### PHP

```php
$df = IO::read_mysql($CONN, 'SELECT * FROM products');
IO::executeMysql($CONN, 'CREATE TABLE IF NOT EXISTS products_bak (id BIGINT, price DOUBLE)');

$rows = $df->to_mysql($CONN, 'products_bak', 'append');
$rows = $df->to_mysql($CONN, 'products_bak', 'upsert', ['id']);
```

### Write modes

| Mode | SQL generated |
|------|--------------|
| `append` | `INSERT INTO table (cols) VALUES (chunk of 1,000 rows)` Auto-upgrades to `LOAD DATA LOCAL INFILE` for > 10,000 rows if `local_infile=ON` |
| `replace` | `REPLACE INTO table (cols) VALUES (chunk of 1,000 rows)` |
| `upsert` | `INSERT INTO table (cols) VALUES … ON DUPLICATE KEY UPDATE col=VALUES(col)…` |

!!! info "Enabling LOAD DATA LOCAL INFILE"
    ```sql
    SET GLOBAL local_infile = 1;
    -- or in my.cnf: local_infile=1
    ```
    If disabled, PardoX automatically falls back to chunked INSERT and logs a notice to stderr.

---

## SQL Server

### Python

```python
from pardox.io import read_sqlserver, execute_sqlserver

CONN = "Server=localhost,1433;Database=mydb;UID=sa;PWD=MyPassword;TrustServerCertificate=Yes"

# Read
df = read_sqlserver(CONN, "SELECT TOP 5000 * FROM dbo.transactions")

# DDL
execute_sqlserver(CONN, "DROP TABLE IF EXISTS dbo.transactions_bak")
execute_sqlserver(CONN, """
    CREATE TABLE dbo.transactions_bak (
        id     BIGINT,
        amount FLOAT,
        region NVARCHAR(MAX)
    )
""")

# Write — batch INSERT 500 rows per statement
rows = df.to_sqlserver(CONN, "dbo.transactions_bak", mode="append")

# Upsert — MERGE INTO … WHEN MATCHED … WHEN NOT MATCHED
rows = df.to_sqlserver(CONN, "dbo.transactions_bak", mode="upsert", conflict_cols=["id"])
```

### Node.js

```js
const { read_sqlserver, execute_sqlserver } = require('pardox/src/io');

const df = read_sqlserver(CONN, 'SELECT TOP 1000 * FROM dbo.orders');
execute_sqlserver(CONN, 'DROP TABLE IF EXISTS dbo.orders_bak');

const rows = df.toSqlserver(CONN, 'dbo.orders_bak', 'append');
const rows2 = df.toSqlserver(CONN, 'dbo.orders_bak', 'upsert', ['id']);
```

### PHP

```php
$df = IO::read_sqlserver($CONN, 'SELECT TOP 1000 * FROM dbo.orders');
IO::executeSqlserver($CONN, 'DROP TABLE IF EXISTS dbo.orders_bak');

$rows = $df->to_sqlserver($CONN, 'dbo.orders_bak', 'append');
$rows = $df->to_sqlserver($CONN, 'dbo.orders_bak', 'upsert', ['id']);
```

### Write modes

| Mode | SQL generated |
|------|--------------|
| `append` | `INSERT INTO table (cols) VALUES (r1),(r2),…,(r500)` — 500 rows per statement |
| `replace` | Same multi-row INSERT |
| `upsert` | `MERGE INTO table AS T USING (VALUES …) AS S (cols) ON (key) WHEN MATCHED … WHEN NOT MATCHED …` |

!!! warning "Password special characters"
    A known issue in tiberius v0.12 causes authentication failure when the SQL Server password contains `!`. Use passwords with only `[A-Za-z0-9_\-@#$]`. A fix is tracked for v0.3.2.

---

## MongoDB

### Python

```python
from pardox.io import read_mongodb, execute_mongodb

CONN = "mongodb://admin:secret@localhost:27017"

# Read — "database.collection"
df = read_mongodb(CONN, "mydb.orders")
print(df.shape)

# Command (admin operations)
execute_mongodb(CONN, "mydb", '{"drop": "orders_archive"}')

# Write — append (insert_many 10k/batch, ordered:false)
rows = df.to_mongodb(CONN, "mydb.orders_archive", mode="append")

# Write — replace (drop collection, then re-insert)
rows = df.to_mongodb(CONN, "mydb.orders_archive", mode="replace")
```

### Node.js

```js
const { read_mongodb, execute_mongodb } = require('pardox/src/io');

const df = read_mongodb(CONN, 'mydb.orders');
execute_mongodb(CONN, 'mydb', '{"drop": "orders_archive"}');

const rows = df.toMongodb(CONN, 'mydb.orders_archive', 'append');
```

### PHP

```php
$df = IO::read_mongodb($CONN, 'mydb.orders');
IO::executeMongodb($CONN, 'mydb', '{"drop": "orders_archive"}');

$rows = $df->to_mongodb($CONN, 'mydb.orders_archive', 'append');
```

### Write modes

| Mode | Behavior |
|------|----------|
| `append` | `insert_many` in batches of 10,000 documents, `ordered: false` |
| `replace` | Drops the collection, then `insert_many` in batches of 10,000 |

---

## Bulk Write Performance

| Database | Rows < 10k | Rows ≥ 10k (append) | Protocol |
|----------|-----------|---------------------|----------|
| PostgreSQL | Multi-row INSERT | **COPY FROM STDIN** (auto) | Wire-level streaming |
| MySQL | 1,000 rows/INSERT | **LOAD DATA LOCAL INFILE** (auto, if `local_infile=ON`) | In-memory CSV buffer |
| SQL Server | 500 rows/INSERT | 500 rows/INSERT | Multi-value INSERT |
| MongoDB | 10,000 docs/batch | 10,000 docs/batch | `insert_many` |

---

## PRDX Streaming to PostgreSQL (v0.3.2)

*New in v0.3.2:* Stream any `.prdx` file directly to PostgreSQL — **without loading the file into RAM**. Uses `COPY FROM STDIN` with block-by-block decompression.

### Python

```python
from pardox import write_sql_prdx

CONN = "postgresql://user:pass@localhost:5434/mydb"

rows = write_sql_prdx(
    "/data/ventas_150m.prdx",
    CONN,
    "ventas",          # table must exist with matching schema
    mode="append",
    conflict_cols=[],
    batch_rows=1_000_000
)
print(f"Streamed {rows:,} rows")
```

### JavaScript

```js
const { write_sql_prdx } = require('./pardox/src/io');

const rows = write_sql_prdx(
    '/data/ventas_150m.prdx',
    'postgresql://user:pass@localhost:5434/mydb',
    'ventas',
    'append',
    [],
    1000000
);
console.log(`Streamed ${rows.toLocaleString()} rows`);
```

### PHP

```php
use PardoX\IO;

$rows = IO::write_sql_prdx(
    '/data/ventas_150m.prdx',
    'postgresql://user:pass@localhost:5434/mydb',
    'ventas',
    'append',
    [],
    1000000
);
echo "Streamed $rows rows\n";
```

### Memory characteristics

| Approach | RAM used |
|----------|----------|
| `df = read_prdx(path)` then `df.to_sql(...)` | Entire file loaded (3.8 GB for 150M rows) |
| `write_sql_prdx(path, ...)` | O(one block × columns) — typically < 200 MB |

---

## Validated Results (v0.3.2)

### PRDX → PostgreSQL — 150M rows (3.8 GB)

| SDK | Rows | Time | Throughput | Protocol |
|-----|------|------|------------|----------|
| Python | 150,000,000 | ~490s | ~306,000 rows/s | COPY FROM STDIN |
| JavaScript | 150,000,000 | ~514s | ~291,000 rows/s | COPY FROM STDIN |
| PHP | 150,000,000 | ~1,032s | ~145,000 rows/s | batch INSERT |

### CSV / DataFrame → PostgreSQL

| Database | SDK | Result |
|----------|-----|--------|
| PostgreSQL | Python | ✅ 50,000 rows — COPY FROM STDIN |
| PostgreSQL | Node.js | ✅ 50,000 rows — COPY FROM STDIN |
| MySQL | Python | ✅ 50,000 rows — chunked batch (LOAD DATA disabled server-side) |
| MySQL | PHP | ✅ 50,000 rows — chunked batch |
| SQL Server | all | ⚠️ Authentication fails with `!` in password |
| MongoDB | — | Implemented; bulk benchmarks planned |
