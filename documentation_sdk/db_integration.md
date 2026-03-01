# PardoX — Database Integration Guide

**Version:** v0.3.1
**SDKs covered:** Python · Node.js · PHP

---

## Overview

PardoX uses native Rust drivers for all database operations. There is no Python/Node.js/PHP database library involved — the host language passes a connection string and a pointer to the DataFrame; the Rust core handles the entire connection, serialization, and transfer. This means:

- Zero Python/JS/PHP dependencies for database I/O
- No ORM or query builder overhead
- Bulk write paths operate at the wire level (COPY, LOAD DATA, batch INSERT)

---

## Supported Databases

| Database    | Rust driver   | Read | Write | Execute DDL/DML |
|-------------|--------------|------|-------|-----------------|
| PostgreSQL  | `tokio-postgres` | ✅ | ✅ | ✅ |
| MySQL       | `mysql` v25  | ✅ | ✅ | ✅ |
| SQL Server  | `tiberius` v0.12 | ✅ | ✅ | ✅ |
| MongoDB     | `mongodb` v2.8 | ✅ | ✅ | ✅ |

---

## Connection String Formats

### PostgreSQL
```
postgresql://user:password@host:port/database
```
Example:
```
postgresql://pardox:pardox123@localhost:5432/pardox_test
```

### MySQL
```
mysql://user:password@host:port/database
```
Example:
```
mysql://pardox:pardoxpassword@localhost:3306/pardox_test
```

### SQL Server (ADO.NET format)
```
Server=host,port;Database=db;UID=user;PWD=password;TrustServerCertificate=Yes
```
Example:
```
Server=localhost,1433;Database=mydb;UID=sa;PWD=MyPassword;TrustServerCertificate=Yes
```
> **Note:** Avoid special characters (particularly `!`) in SQL Server passwords. See [Known Limitations](#known-limitations).

### MongoDB
```
mongodb://user:password@host:port
```
Example:
```
mongodb://admin:secret@localhost:27017
```

---

## PostgreSQL

### Python

```python
from pardox.io import read_sql, execute_sql
import pardox

# Read
df = read_sql("postgresql://pardox:pardox123@localhost:5432/mydb", "SELECT * FROM orders")

# Execute DDL / DML
execute_sql("postgresql://...", "DROP TABLE IF EXISTS orders")
execute_sql("postgresql://...", "CREATE TABLE orders (id BIGINT, amount FLOAT)")

# Write — append (auto-activates COPY FROM STDIN for > 10,000 rows)
rows = df.to_sql("postgresql://...", "orders", mode="append")

# Write — upsert (INSERT … ON CONFLICT DO UPDATE)
rows = df.to_sql("postgresql://...", "orders", mode="upsert", conflict_cols=["id"])
```

### Node.js

```js
const { read_sql, executeSql } = require('./pardox/src/io');
const { DataFrame } = require('./pardox/src/DataFrame');

// Read
const df = read_sql('postgresql://pardox:pardox123@localhost:5432/mydb', 'SELECT * FROM orders');

// Execute DDL / DML
executeSql('postgresql://...', 'DROP TABLE IF EXISTS orders');
executeSql('postgresql://...', 'CREATE TABLE orders (id BIGINT, amount FLOAT)');

// Write — append
const rows = df.toSql('postgresql://...', 'orders', 'append');

// Write — upsert
const rows = df.toSql('postgresql://...', 'orders', 'upsert', ['id']);
```

### PHP

```php
use PardoX\DataFrame;
use PardoX\IO;

// Read
$df = IO::read_sql('postgresql://pardox:pardox123@localhost:5432/mydb', 'SELECT * FROM orders');

// Execute DDL / DML
IO::executeSql('postgresql://...', 'DROP TABLE IF EXISTS orders');
IO::executeSql('postgresql://...', 'CREATE TABLE orders (id BIGINT, amount FLOAT)');

// Write — append
$rows = $df->to_sql('postgresql://...', 'orders', 'append');

// Write — upsert
$rows = $df->to_sql('postgresql://...', 'orders', 'upsert', ['id']);
```

### Write Modes — PostgreSQL

| Mode | Behavior |
|------|----------|
| `append` | `INSERT INTO … VALUES …`. Auto-upgrades to `COPY FROM STDIN` when rows > 10,000. |
| `upsert` | `INSERT … ON CONFLICT (cols) DO UPDATE SET …`. Requires `conflict_cols`. |

### Bulk Performance — PostgreSQL

For DataFrames with more than 10,000 rows and `mode="append"`, the Rust core automatically switches to `COPY FROM STDIN`. The CSV data is serialized in memory and streamed directly into PostgreSQL — no temp files.

---

## MySQL

### Python

```python
from pardox.io import read_mysql, execute_mysql
import pardox

CONN = "mysql://pardox:pardoxpassword@localhost:3306/mydb"

# Read
df = read_mysql(CONN, "SELECT * FROM sales")

# Execute DDL / DML
execute_mysql(CONN, "DROP TABLE IF EXISTS sales")
execute_mysql(CONN, (
    "CREATE TABLE sales ("
    "id BIGINT, amount DOUBLE, category TEXT)"
))

# Write — append
rows = df.to_mysql(CONN, "sales", mode="append")

# Write — upsert (ON DUPLICATE KEY UPDATE)
rows = df.to_mysql(CONN, "sales", mode="upsert", conflict_cols=["id"])
```

### Node.js

```js
const { read_mysql, execute_mysql } = require('./pardox/src/io');

const CONN = 'mysql://pardox:pardoxpassword@localhost:3306/mydb';

// Read
const df = read_mysql(CONN, 'SELECT * FROM sales');

// Execute DDL / DML
execute_mysql(CONN, 'DROP TABLE IF EXISTS sales');
execute_mysql(CONN, 'CREATE TABLE sales (id BIGINT, amount DOUBLE, category TEXT)');

// Write — append
const rows = df.toMysql(CONN, 'sales', 'append');

// Write — replace (DELETE existing rows then INSERT)
const rows = df.toMysql(CONN, 'sales', 'replace');

// Write — upsert
const rows = df.toMysql(CONN, 'sales', 'upsert', ['id']);
```

### PHP

```php
use PardoX\DataFrame;
use PardoX\IO;

$CONN = 'mysql://pardox:pardoxpassword@localhost:3306/mydb';

// Read
$df = IO::read_mysql($CONN, 'SELECT * FROM sales');

// Execute DDL / DML
IO::executeMysql($CONN, 'DROP TABLE IF EXISTS sales');
IO::executeMysql($CONN, 'CREATE TABLE IF NOT EXISTS sales (id BIGINT, amount DOUBLE, category TEXT)');

// Write — append
$rows = $df->to_mysql($CONN, 'sales', 'append');

// Write — upsert
$rows = $df->to_mysql($CONN, 'sales', 'upsert', ['id']);
```

### Write Modes — MySQL

| Mode | Behavior |
|------|----------|
| `append` | `INSERT INTO … VALUES …` in chunks of 1,000 rows per statement. Auto-upgrades to `LOAD DATA LOCAL INFILE` when rows > 10,000 **and** the server has `local_infile=ON`. |
| `replace` | `REPLACE INTO … VALUES …` in chunks of 1,000 rows. |
| `upsert` | `INSERT … ON DUPLICATE KEY UPDATE`. Requires `conflict_cols`. |

### Bulk Performance — MySQL

When `local_infile` is enabled on the MySQL server, appending more than 10,000 rows automatically uses `LOAD DATA LOCAL INFILE`. The CSV payload is built in memory and delivered via the MySQL binary protocol — no disk files. If the server has `local_infile=OFF` (ERROR 3948), the Rust core falls back to the 1,000-row chunked INSERT automatically, logging a notice to stderr.

---

## SQL Server

### Python

```python
from pardox.io import read_sqlserver, execute_sqlserver
import pardox

CONN = "Server=localhost,1433;Database=mydb;UID=sa;PWD=MyPassword;TrustServerCertificate=Yes"

# Read
df = read_sqlserver(CONN, "SELECT * FROM dbo.orders")

# Execute DDL / DML
execute_sqlserver(CONN, "DROP TABLE IF EXISTS dbo.orders")
execute_sqlserver(CONN, (
    "CREATE TABLE dbo.orders ("
    "id BIGINT, amount FLOAT, category NVARCHAR(MAX))"
))

# Write — append (batch INSERT 500 rows/statement)
rows = df.to_sqlserver(CONN, "dbo.orders", mode="append")

# Write — upsert (MERGE … WHEN MATCHED … WHEN NOT MATCHED …)
rows = df.to_sqlserver(CONN, "dbo.orders", mode="upsert", conflict_cols=["id"])
```

### Node.js

```js
const { read_sqlserver, execute_sqlserver } = require('./pardox/src/io');

const CONN = 'Server=localhost,1433;Database=mydb;UID=sa;PWD=MyPassword;TrustServerCertificate=Yes';

// Read
const df = read_sqlserver(CONN, 'SELECT * FROM dbo.orders');

// Execute DDL / DML
execute_sqlserver(CONN, 'DROP TABLE IF EXISTS dbo.orders');

// Write — append
const rows = df.toSqlserver(CONN, 'dbo.orders', 'append');

// Write — upsert (MERGE)
const rows = df.toSqlserver(CONN, 'dbo.orders', 'upsert', ['id']);
```

### PHP

```php
use PardoX\DataFrame;
use PardoX\IO;

$CONN = 'Server=localhost,1433;Database=mydb;UID=sa;PWD=MyPassword;TrustServerCertificate=Yes';

// Read
$df = IO::read_sqlserver($CONN, 'SELECT * FROM dbo.orders');

// Execute DDL / DML
IO::executeSqlserver($CONN, 'DROP TABLE IF EXISTS dbo.orders');

// Write — append
$rows = $df->to_sqlserver($CONN, 'dbo.orders', 'append');

// Write — upsert (MERGE)
$rows = $df->to_sqlserver($CONN, 'dbo.orders', 'upsert', ['id']);
```

### Write Modes — SQL Server

| Mode | Behavior |
|------|----------|
| `append` | `INSERT INTO table (cols) VALUES (r1),(r2),…,(r500)` — 500 rows per statement. |
| `replace` | Same multi-row INSERT, replacing existing rows. |
| `upsert` | `MERGE INTO table AS T USING (VALUES …) AS S ON (…) WHEN MATCHED … WHEN NOT MATCHED …` — 500 rows per MERGE statement. Requires `conflict_cols`. |

---

## MongoDB

### Python

```python
from pardox.io import read_mongodb, execute_mongodb
import pardox

CONN = "mongodb://admin:secret@localhost:27017"

# Read — collection as "database.collection"
df = read_mongodb(CONN, "mydb.orders")

# Execute a MongoDB command
execute_mongodb(CONN, "mydb", '{"drop": "orders"}')

# Write — append
rows = df.to_mongodb(CONN, "mydb.orders", mode="append")

# Write — replace (drops collection, then inserts)
rows = df.to_mongodb(CONN, "mydb.orders", mode="replace")
```

### Node.js

```js
const { read_mongodb, execute_mongodb } = require('./pardox/src/io');

const CONN = 'mongodb://admin:secret@localhost:27017';

// Read
const df = read_mongodb(CONN, 'mydb.orders');

// Execute command
execute_mongodb(CONN, 'mydb', '{"drop": "orders"}');

// Write — append
const rows = df.toMongodb(CONN, 'mydb.orders', 'append');

// Write — replace
const rows = df.toMongodb(CONN, 'mydb.orders', 'replace');
```

### PHP

```php
use PardoX\DataFrame;
use PardoX\IO;

$CONN = 'mongodb://admin:secret@localhost:27017';

// Read
$df = IO::read_mongodb($CONN, 'mydb.orders');

// Execute command
IO::executeMongodb($CONN, 'mydb', '{"drop": "orders"}');

// Write — append
$rows = $df->to_mongodb($CONN, 'mydb.orders', 'append');

// Write — replace
$rows = $df->to_mongodb($CONN, 'mydb.orders', 'replace');
```

### Write Modes — MongoDB

| Mode | Behavior |
|------|----------|
| `append` | `insert_many` in batches of 10,000 documents, `ordered: false` (continues on individual document errors). |
| `replace` | Drops the target collection, then inserts all documents in batches of 10,000. |

---

## API Reference Summary

### Python (`pardox.io`)

| Function | Returns | Description |
|----------|---------|-------------|
| `read_sql(conn, query)` | `DataFrame` | Read from PostgreSQL |
| `execute_sql(conn, query)` | `int` | DDL/DML on PostgreSQL |
| `read_mysql(conn, query)` | `DataFrame` | Read from MySQL |
| `execute_mysql(conn, query)` | `int` | DDL/DML on MySQL |
| `read_sqlserver(conn, query)` | `DataFrame` | Read from SQL Server |
| `execute_sqlserver(conn, query)` | `int` | DDL/DML on SQL Server |
| `read_mongodb(conn, db.col)` | `DataFrame` | Read a MongoDB collection |
| `execute_mongodb(conn, db, cmd_json)` | `int` | Run a MongoDB command |

### DataFrame write methods — Python

| Method | Returns | Description |
|--------|---------|-------------|
| `df.to_sql(conn, table, mode, conflict_cols)` | `int` | Write to PostgreSQL |
| `df.to_mysql(conn, table, mode, conflict_cols)` | `int` | Write to MySQL |
| `df.to_sqlserver(conn, table, mode, conflict_cols)` | `int` | Write to SQL Server |
| `df.to_mongodb(conn, db.col, mode)` | `int` | Write to MongoDB |

### Node.js (`pardox/src/io.js`)

| Function | Returns | Description |
|----------|---------|-------------|
| `read_sql(conn, query)` | `DataFrame` | Read from PostgreSQL |
| `executeSql(conn, query)` | `number` | DDL/DML on PostgreSQL |
| `read_mysql(conn, query)` | `DataFrame` | Read from MySQL |
| `execute_mysql(conn, query)` | `number` | DDL/DML on MySQL |
| `read_sqlserver(conn, query)` | `DataFrame` | Read from SQL Server |
| `execute_sqlserver(conn, query)` | `number` | DDL/DML on SQL Server |
| `read_mongodb(conn, db.col)` | `DataFrame` | Read a MongoDB collection |
| `execute_mongodb(conn, db, cmd_json)` | `number` | Run a MongoDB command |

### DataFrame write methods — Node.js

| Method | Returns | Description |
|--------|---------|-------------|
| `df.toSql(conn, table, mode, conflictCols)` | `number` | Write to PostgreSQL |
| `df.toMysql(conn, table, mode, conflictCols)` | `number` | Write to MySQL |
| `df.toSqlserver(conn, table, mode, conflictCols)` | `number` | Write to SQL Server |
| `df.toMongodb(conn, db.col, mode)` | `number` | Write to MongoDB |

### PHP (`PardoX\IO` + `PardoX\DataFrame`)

| Function | Returns | Description |
|----------|---------|-------------|
| `IO::read_sql($conn, $query)` | `DataFrame` | Read from PostgreSQL |
| `IO::executeSql($conn, $query)` | `int` | DDL/DML on PostgreSQL |
| `IO::read_mysql($conn, $query)` | `DataFrame` | Read from MySQL |
| `IO::executeMysql($conn, $query)` | `int` | DDL/DML on MySQL |
| `IO::read_sqlserver($conn, $query)` | `DataFrame` | Read from SQL Server |
| `IO::executeSqlserver($conn, $query)` | `int` | DDL/DML on SQL Server |
| `IO::read_mongodb($conn, $db_col)` | `DataFrame` | Read a MongoDB collection |
| `IO::executeMongodb($conn, $db, $cmd_json)` | `int` | Run a MongoDB command |

### DataFrame write methods — PHP

| Method | Returns | Description |
|--------|---------|-------------|
| `$df->to_sql($conn, $table, $mode, $conflictCols)` | `int` | Write to PostgreSQL |
| `$df->to_mysql($conn, $table, $mode, $conflictCols)` | `int` | Write to MySQL |
| `$df->to_sqlserver($conn, $table, $mode, $conflictCols)` | `int` | Write to SQL Server |
| `$df->to_mongodb($conn, $db_col, $mode)` | `int` | Write to MongoDB |

---

## Return Values

All write methods return the number of rows written as an integer. All execute methods return the number of rows affected (0 for DDL statements like `CREATE TABLE` or `DROP TABLE`). On error, all methods throw an exception (Python: `RuntimeError`, Node.js: `Error`, PHP: `RuntimeException`).

---

## Bulk Write Performance Summary

| Database | Rows < 10,000 | Rows ≥ 10,000 (append) | Mechanism |
|----------|--------------|------------------------|-----------|
| PostgreSQL | Multi-row INSERT | **COPY FROM STDIN** (auto) | Wire-level streaming, no temp files |
| MySQL | 1,000 rows/stmt | **LOAD DATA LOCAL INFILE** (auto, if server allows) → fallback to 1,000 rows/stmt | In-memory CSV buffer |
| SQL Server | 500 rows/stmt | 500 rows/stmt | Multi-row `INSERT … VALUES` |
| MongoDB | 10,000 docs/batch | 10,000 docs/batch | `insert_many(ordered: false)` |

---

## Known Limitations

### SQL Server — Special Characters in Password
Passwords containing `!` (and potentially other special characters) cause the tiberius driver to send an incorrect authentication hash, resulting in a `Login failed (State 8)` error. This affects all SDKs since the issue is in the Rust core.

**Workaround:** Use a SQL Server password that contains only alphanumeric characters and `_`, `-`, `@`, `#`, `$`.

```
# Works
Server=localhost,1433;UID=sa;PWD=MyPassword_123;TrustServerCertificate=Yes

# Fails (tiberius bug with !)
Server=localhost,1433;UID=sa;PWD=MyP@ssw0rd!;TrustServerCertificate=Yes
```

A fix is tracked for the next release — see `validation_complete/suggestions.md`.

### MySQL — LOAD DATA LOCAL INFILE Requires Server Configuration
The fast bulk path (`LOAD DATA LOCAL INFILE`) requires `local_infile=ON` on the MySQL server. If it is disabled (ERROR 3948), PardoX automatically falls back to chunked batch INSERT with no action required from the user.

To enable on the server side:
```sql
SET GLOBAL local_infile = 1;
```
Or in `my.cnf`:
```ini
[mysqld]
local_infile=1
```

### Table Must Exist Before Writing
PardoX write methods (`to_sql`, `to_mysql`, etc.) append to an existing table. Create the table first using the corresponding `execute_*` function before calling a write method.

```python
execute_sql(CONN, "CREATE TABLE IF NOT EXISTS orders (id BIGINT, amount FLOAT)")
rows = df.to_sql(CONN, "orders", mode="append")
```

---

## Validated Versions (v0.3.1)

| Database | Read | Write (50k rows) | SDK |
|----------|------|------------------|-----|
| PostgreSQL | ✅ | ✅ COPY (0.x s) | Python, Node.js |
| MySQL | ✅ | ✅ chunked batch | Python, PHP |
| SQL Server | ✅ | ⚠️ password `!` bug | — |
| MongoDB | ✅ | not benchmarked | — |
