# PardoX for Node.js — High-Performance DataFrame Engine

[![npm version](https://badge.fury.io/js/%40pardox%2Fpardox.svg)](https://www.npmjs.com/package/@pardox/pardox)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Node.js 18+](https://img.shields.io/badge/node-18+-339933.svg)](https://nodejs.org/)
[![Powered By Rust](https://img.shields.io/badge/powered%20by-Rust-orange.svg)](https://www.rust-lang.org/)

**The Speed of Rust. The Simplicity of JavaScript.**

PardoX is a high-performance DataFrame engine for Node.js. A single **Rust core** handles all computation — CSV parsing, arithmetic, database I/O, sorting, GroupBy, Window functions, and more — exposed to JavaScript through [koffi](https://koffi.dev/) FFI. No Python. No native Node addons to compile. No database drivers.

> **v0.3.4 is now available.** SQL Cursor API (Gap 30) — `queryToResults()` streaming iterator validated in 11/11 tests. Plus: PRDX streaming to PostgreSQL (150M rows validated), GroupBy, String & Date ops, Window functions, Lazy pipeline, SQL over DataFrames, WebAssembly, Encryption, Data Contracts, Time Travel, Arrow Flight, Linear Algebra, REST Connector — all from Node.js.

---

## ⚡ Why PardoX for Node.js?

| Capability | How |
|------------|-----|
| **Zero-copy ingestion** | Multi-threaded Rust CSV parser — no JS string processing |
| **SIMD arithmetic** | AVX2 / NEON — 5x–20x faster than JS loops |
| **Native database I/O** | Rust drivers for PostgreSQL, MySQL, SQL Server, MongoDB — no `pg`, no `mysql2`, no Mongoose |
| **PRDX Streaming** | Stream 150M-row files to PostgreSQL at ~291k rows/s with O(block) RAM |
| **GPU sort** | WebGPU Bitonic sort with transparent CPU fallback |
| **GroupBy + Window** | Aggregations, rolling, rank, lag/lead — pure Rust |
| **WebAssembly** | Run PardoX in the browser via WASM target |
| **Proxy-based subscript** | `df['col']` returns a Series; `df['col'] = series` assigns a column |
| **Cross-platform** | Linux x64 · Windows x64 · macOS Intel · macOS Apple Silicon |

---

## 📦 Installation

```bash
npm install @pardox/pardox
```

**Requirements:**
- Node.js 18 or higher
- No native compilation needed — prebuilt Rust binaries included for all platforms

---

## 🚀 Quick Start

```js
const { DataFrame, read_csv, executeSql } = require('@pardox/pardox');

// Load 100,000 rows — parallel Rust CSV parser
const df = read_csv('./sales_data.csv');
console.log(`Loaded ${df.shape[0].toLocaleString()} rows × ${df.shape[1]} columns`);

// SIMD-accelerated arithmetic
const revenueDf = df.mul('price', 'quantity');   // result column: 'result_mul'

// Statistics — pure Rust
console.log(`Total revenue : $${df['price'].sum().toFixed(2)}`);
console.log(`Avg ticket    : $${df['price'].mean().toFixed(2)}`);

// GroupBy aggregation
const grouped = df.groupBy('state', { revenue: 'sum', quantity: 'mean' });

// Write to PostgreSQL — COPY FROM STDIN auto-activated for > 10k rows
const PG = 'postgresql://user:password@localhost:5432/mydb';
executeSql(PG, 'CREATE TABLE IF NOT EXISTS sales (price FLOAT, quantity FLOAT)');
const rows = df.toSql(PG, 'sales', 'append');
console.log(`Written ${rows.toLocaleString()} rows to PostgreSQL`);

// Save locally — 4.6 GB/s read throughput
df.toPrdx('./sales_processed.prdx');
```

---

## 🗄️ What's New in v0.3.2

### PRDX Streaming to PostgreSQL

Stream a `.prdx` file directly to PostgreSQL — **without loading the file into RAM**. O(block) memory regardless of file size.

```js
const { write_sql_prdx } = require('@pardox/pardox');

const rows = write_sql_prdx(
    '/data/ventas_150m.prdx',
    'postgresql://user:pass@localhost:5434/mydb',
    'ventas',       // table must already exist
    'append',
    [],
    1000000         // rows per COPY batch
);
console.log(`Streamed ${rows.toLocaleString()} rows`);
// Validated: 150,000,000 rows / 3.8 GB in ~514s at ~291k rows/s
```

| Approach | RAM used |
|----------|----------|
| `read_prdx()` then `toSql()` | Entire file (3.8 GB for 150M rows) |
| `write_sql_prdx()` | O(one block) — typically < 200 MB |

---

### GroupBy Aggregation

```js
// Single aggregation
const grouped = df.groupBy('category', { revenue: 'sum' });

// Multiple aggregations
const grouped = df.groupBy('state', {
    revenue:  'sum',
    price:    'mean',
    quantity: 'count',
});
```

---

### String & Date Operations

```js
// String ops
df.strUpper('name');
df.strLower('email');
df.strTrim('description');
df.strContains('tag', 'node');
df.strReplace('status', 'old', 'new');

// Date ops
df.dateExtract('created_at', 'year');    // → 'result_year'
df.dateFormat('created_at', '%Y-%m');
df.dateDiff('end_date', 'start_date');
df.dateAdd('created_at', 30, 'day');
```

---

### Window Functions

```js
df.rowNumber('price');
df.rank('revenue', 'dense');
df.lag('price', 1);
df.lead('price', 1);
df.rollingMean('price', 7);   // 7-period moving average
```

---

### Lazy Pipeline

```js
const { lazy_scan_csv } = require('@pardox/pardox');

// Scan without loading — filter and collect on demand
const result = lazy_scan_csv('./large_file.csv')
    .select(['id', 'price', 'state'])
    .filter('price', '>', 100.0)
    .limit(10000)
    .collect();

console.log(`${result.shape[0]} rows`);
```

---

### SQL over DataFrames

```js
// Run SQL directly on a DataFrame
const result = df.sql('SELECT state, SUM(revenue) as total FROM df GROUP BY state');
```

---

### Cloud Storage

```js
const { cloud_read_csv } = require('@pardox/pardox');

// Read CSV from S3, GCS, Azure, or local file://
const df = cloud_read_csv(
    's3://my-bucket/data.csv',
    '{}',               // schema (empty = auto-detect)
    '{}',               // config
    JSON.stringify({ access_key_id: '...', secret_access_key: '...' })
);
```

---

## 🗄️ Database I/O

```js
const {
    read_csv,
    read_sql,    executeSql,
    read_mysql,  execute_mysql,
    read_sqlserver, execute_sqlserver,
    read_mongodb,   execute_mongodb,
} = require('@pardox/pardox');

// ── PostgreSQL ───────────────────────────────────────────────
const PG = 'postgresql://user:pass@localhost:5432/db';

const df = read_sql(PG, "SELECT * FROM orders WHERE status = 'active'");
executeSql(PG, 'CREATE TABLE orders_archive (id BIGINT, amount FLOAT, region TEXT)');
df.toSql(PG, 'orders_archive', 'append');          // COPY FROM STDIN for > 10k rows
df.toSql(PG, 'orders_archive', 'upsert', ['id']); // ON CONFLICT DO UPDATE

// ── MySQL ────────────────────────────────────────────────────
const MY = 'mysql://user:pass@localhost:3306/db';

const dfMy = read_mysql(MY, 'SELECT * FROM products WHERE active = 1');
execute_mysql(MY, 'CREATE TABLE IF NOT EXISTS products_bak (id BIGINT, price DOUBLE)');
dfMy.toMysql(MY, 'products_bak', 'append');
dfMy.toMysql(MY, 'products_bak', 'upsert', ['id']);

// ── SQL Server ───────────────────────────────────────────────
const MS = 'Server=localhost,1433;Database=mydb;UID=sa;PWD=MyPwd;TrustServerCertificate=Yes';

const dfMs = read_sqlserver(MS, 'SELECT TOP 5000 * FROM dbo.transactions');
dfMs.toSqlserver(MS, 'dbo.transactions_bak', 'upsert', ['id']); // MERGE INTO

// ── MongoDB ──────────────────────────────────────────────────
const MG = 'mongodb://admin:pass@localhost:27017';

const dfMg = read_mongodb(MG, 'mydb.orders');
dfMg.toMongodb(MG, 'mydb.orders_archive', 'append');   // 10k docs/batch
```

**Write modes:**

| Database | `append` | `replace` | `upsert` |
|----------|----------|-----------|----------|
| PostgreSQL | INSERT (COPY for >10k rows) | — | ON CONFLICT DO UPDATE |
| MySQL | INSERT 1k/stmt (LOAD DATA for >10k) | REPLACE INTO | ON DUPLICATE KEY UPDATE |
| SQL Server | INSERT 500/stmt | INSERT 500/stmt | MERGE INTO |
| MongoDB | insert_many 10k/batch | drop + insert_many | — |

> **Note on SQL Server passwords:** Avoid using `!` in SQL Server passwords. A known issue in the tiberius v0.12 Rust driver causes authentication failure when `!` is present. Use only `[A-Za-z0-9_\-@#$]`. Fix planned for v0.4.0.

---

## 📋 Full API Overview

### Top-level functions

```js
const {
    DataFrame, Series,
    read_csv, read_prdx,
    read_sql,    executeSql,
    read_mysql,  execute_mysql,
    read_sqlserver, execute_sqlserver,
    read_mongodb,   execute_mongodb,
    write_sql_prdx,
    lazy_scan_csv,
    cloud_read_csv,
} = require('@pardox/pardox');
```

### DataFrame — Properties

```js
df.shape      // [rows, cols]
df.columns    // ['col1', 'col2', ...]
df.dtypes     // { col1: 'Float64', ... }
```

### DataFrame — Inspection

```js
df.show(10);              // ASCII table (stdout)
df.head(5);               // → DataFrame
df.tail(5);               // → DataFrame
df.iloc(0, 100);          // → DataFrame (rows 0–99)
```

### DataFrame — Arithmetic & Transform

```js
df.cast('quantity', 'Float64');
df.fillna(0.0);
df.round(2);
df.mul('price', 'quantity');           // → DataFrame with 'result_mul'
df.sub('revenue', 'cost');             // → DataFrame with 'result_math_sub'
df.add('amount', 'tax');               // → DataFrame with 'result_math_add'
df.std('column');                      // float
df.minMaxScale('col');                 // → DataFrame with 'result_minmax'
df.sortValues('col', true);            // → DataFrame (ascending)
df.sortValues('col', false, true);     // → DataFrame (descending, GPU)
```

### DataFrame — GroupBy & Aggregation

```js
df.groupBy('category', { revenue: 'sum', price: 'mean' });
df.groupBy('state', { quantity: 'count', revenue: 'max' });
```

### DataFrame — Window Functions

```js
df.rowNumber('price');
df.rank('revenue', 'dense');
df.lag('price', 1);
df.lead('price', 1);
df.rollingMean('price', 7);
```

### DataFrame — String Operations

```js
df.strUpper('col');
df.strLower('col');
df.strTrim('col');
df.strLen('col');
df.strContains('col', 'pattern');
df.strReplace('col', 'old', 'new');
```

### DataFrame — Join & Filter

```js
df.join(other, { on: 'id' });
df.join(other, { leftOn: 'order_id', rightOn: 'id' });
df.filter(mask);   // → DataFrame
```

### Series — Column Access & Arithmetic

```js
const col = df['price'];                          // → Series
const rev = df['price'].mul(df['quantity']);       // → Series
df['revenue'] = df['price'].mul(df['quantity']);   // column assignment
```

### Series — Aggregations

```js
df['col'].sum();    // float
df['col'].mean();   // float
df['col'].min();    // float
df['col'].max();    // float
df['col'].std();    // float
df['col'].count();  // int
```

### Series — Comparisons (filter masks)

```js
df['price'].eq(100);   df['price'].neq(100);
df['price'].gt(100);   df['price'].gte(100);
df['price'].lt(100);   df['price'].lte(100);
```

### Observer

```js
df.valueCounts('col');   // { 'value': count, ... }
df.unique('col');        // ['val1', 'val2', ...]
df.toDict();             // [{ col: val, ... }, ...]
df.toList();             // [[val, val, ...], ...]
df.toJson();             // JSON string
```

### Write

```js
df.toPrdx('./out.prdx');
df.toCsv('./out.csv');
df.toSql(pgConn, 'table', 'append', ['id']);
df.toMysql(myConn, 'table', 'upsert', ['id']);
df.toSqlserver(msConn, 'dbo.table', 'append');
df.toMongodb(mgConn, 'db.collection', 'append');
write_sql_prdx('file.prdx', pgConn, 'table', 'append', [], 1000000);
```

---

## 📊 Benchmarks

| Operation | Node.js (js-native) | PardoX v0.3.2 | Speedup |
|-----------|---------------------|---------------|---------|
| Read CSV (1 GB) | ~10s | ~0.8s | **~12x** |
| Column multiply (1M rows) | ~0.6s | ~0.02s | **~30x** |
| PostgreSQL write 50k rows | ~20s (pg execute) | ~0.6s (COPY) | **~33x** |
| MySQL write 50k rows | ~25s (mysql2) | ~3s (batch INSERT) | **~8x** |
| PRDX → PostgreSQL 150M rows | N/A | ~514s | 291k rows/s |

---

## 🗺️ Roadmap

| Version | Status | Highlights |
|---------|--------|------------|
| v0.1 | ✅ Released | CSV, arithmetic, aggregations, .prdx format |
| v0.3.1 | ✅ Released | Databases (PG/MySQL/MSSQL/MongoDB), Observer, Math, GPU sort |
| v0.3.2 | ✅ Released | PRDX Streaming, GroupBy, Window, String/Date, Lazy, WebAssembly, Encryption, Data Contracts, Time Travel, Arrow Flight, Linear Algebra, REST Connector — 29 features |
| v0.4.0 | 🔜 Planned | SQL Server `!` password fix, structured error codes, Apache Parquet, Kafka, S3, Gaps 7–11 JS fix |

---

## 🌐 Platform Support

| OS | Architecture | Status |
|----|-------------|--------|
| Linux | x86_64 | ✅ Stable |
| Windows | x86_64 | ✅ Stable |
| macOS | ARM64 (M1/M2/M3) | ✅ Stable |
| macOS | x86_64 (Intel) | ✅ Stable |
| WebAssembly | Browser / Edge | ✅ Stable |

---

## 📘 Documentation

[**Full Documentation →**](https://www.pardox.io)

---

## 📄 License

MIT License — free for commercial and personal use.

---

<p align="center">by Alberto Cardenas<br>
<a href="https://www.albertocardenas.com">www.albertocardenas.com</a> · <a href="https://www.pardox.io">www.pardox.io</a></p>
