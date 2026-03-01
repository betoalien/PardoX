# PardoX for Node.js — High-Performance DataFrame Engine

[![npm version](https://badge.fury.io/js/%40pardox%2Fpardox.svg)](https://www.npmjs.com/package/@pardox/pardox)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Node.js 18+](https://img.shields.io/badge/node-18+-339933.svg)](https://nodejs.org/)
[![Powered By Rust](https://img.shields.io/badge/powered%20by-Rust-orange.svg)](https://www.rust-lang.org/)

**The Speed of Rust. The Simplicity of JavaScript.**

PardoX is a high-performance DataFrame engine for Node.js. A single **Rust core** handles all computation — CSV parsing, arithmetic, database I/O, and sorting — exposed to JavaScript through [koffi](https://koffi.dev/) FFI. No Python. No native Node addons to compile. No database drivers.

> **v0.3.1 is now available.** Native database connectivity for PostgreSQL, MySQL, SQL Server, and MongoDB. Full Observer export. GPU sort. All from Node.js.

---

## ⚡ Why PardoX for Node.js?

| Capability | How |
|------------|-----|
| **Zero-copy ingestion** | Multi-threaded Rust CSV parser — no JS string processing |
| **SIMD arithmetic** | AVX2 / NEON — 5x–20x faster than JS loops |
| **Native database I/O** | Rust drivers for PostgreSQL, MySQL, SQL Server, MongoDB — no `pg`, no `mysql2`, no Mongoose |
| **GPU sort** | WebGPU Bitonic sort with transparent CPU fallback |
| **Proxy-based subscript** | `df['col']` returns a Series; `df['col'] = series` assigns a column — native JS syntax |
| **Cross-platform** | Linux x64 · Windows x64 · macOS Intel · macOS Apple Silicon |

---

## 📦 Installation

```bash
npm install @pardox/pardox
```

**Requirements:**
- Node.js 18 or higher
- No native compilation needed — prebuilt Rust binaries included

---

## 🚀 Quick Start

```js
const { DataFrame, read_csv, executeSql } = require('@pardox/pardox');

// Load 50,000 rows — parallel Rust CSV parser
const df = read_csv('./sales_data.csv');
console.log(`Loaded ${df.shape[0].toLocaleString()} rows × ${df.shape[1]} columns`);

// Cast and compute revenue (SIMD-accelerated)
df.cast('quantity', 'Float64');
const revenueDf = df.mul('price', 'quantity');   // result column: 'result_mul'

// Statistics — pure Rust, no math libraries needed
console.log(`Total revenue : $${df['price'].sum().toFixed(2)}`);
console.log(`Avg ticket    : $${df['price'].mean().toFixed(2)}`);
console.log(`Std deviation : ${df.std('price').toFixed(2)}`);

// Value frequency table
const stateCounts = df.valueCounts('state');
console.log(stateCounts);   // { TX: 6345, CA: 6301, ... }

// Write to PostgreSQL — COPY FROM STDIN auto-activated for > 10k rows
const PG = 'postgresql://user:password@localhost:5432/mydb';
executeSql(PG, 'CREATE TABLE IF NOT EXISTS sales (price FLOAT, quantity FLOAT)');
const rows = df.toSql(PG, 'sales', 'append');
console.log(`Written ${rows.toLocaleString()} rows to PostgreSQL`);

// Save locally — 4.6 GB/s read throughput
df.toPrdx('./sales_processed.prdx');
```

---

## 🗄️ What's New in v0.3.1

### 1. Relational Conqueror — Native Database I/O

Connect to **PostgreSQL, MySQL, SQL Server, and MongoDB** entirely through the Rust core. No `pg`, no `mysql2`, no MongoDB Node driver installed or imported.

```js
const {
    read_csv,
    read_sql, executeSql,                    // PostgreSQL
    read_mysql, execute_mysql,               // MySQL
    read_sqlserver, execute_sqlserver,       // SQL Server
    read_mongodb, execute_mongodb,           // MongoDB
} = require('@pardox/pardox');

// ── PostgreSQL ──────────────────────────────────────────────────────
const PG = 'postgresql://user:pass@localhost:5432/db';

const df = read_sql(PG, "SELECT * FROM orders WHERE status = 'active'");
executeSql(PG, 'DROP TABLE IF EXISTS orders_archive');
executeSql(PG, 'CREATE TABLE orders_archive (id BIGINT, amount FLOAT, region TEXT)');

// COPY FROM STDIN auto-activated for > 10,000 rows
const rowsPg = df.toSql(PG, 'orders_archive', 'append');

// Upsert — INSERT … ON CONFLICT DO UPDATE
const rowsUp = df.toSql(PG, 'orders_archive', 'upsert', ['id']);

// ── MySQL ───────────────────────────────────────────────────────────
const MY = 'mysql://user:pass@localhost:3306/db';

const dfMy = read_mysql(MY, 'SELECT * FROM products WHERE active = 1');
execute_mysql(MY, 'CREATE TABLE IF NOT EXISTS products_bak (id BIGINT, price DOUBLE)');

dfMy.toMysql(MY, 'products_bak', 'append');           // chunked INSERT 1k/stmt
dfMy.toMysql(MY, 'products_bak', 'replace');          // REPLACE INTO
dfMy.toMysql(MY, 'products_bak', 'upsert', ['id']);   // ON DUPLICATE KEY UPDATE

// ── SQL Server ──────────────────────────────────────────────────────
const MS = 'Server=localhost,1433;Database=mydb;UID=sa;PWD=MyPwd;TrustServerCertificate=Yes';

const dfMs = read_sqlserver(MS, 'SELECT TOP 5000 * FROM dbo.transactions');
execute_sqlserver(MS, 'DROP TABLE IF EXISTS dbo.transactions_bak');

dfMs.toSqlserver(MS, 'dbo.transactions_bak', 'append');          // 500 rows/stmt batch INSERT
dfMs.toSqlserver(MS, 'dbo.transactions_bak', 'upsert', ['id']); // MERGE INTO

// ── MongoDB ─────────────────────────────────────────────────────────
const MG = 'mongodb://admin:pass@localhost:27017';

const dfMg = read_mongodb(MG, 'mydb.orders');
execute_mongodb(MG, 'mydb', '{"drop": "orders_archive"}');

dfMg.toMongodb(MG, 'mydb.orders_archive', 'append');   // 10k docs/batch, ordered:false
dfMg.toMongodb(MG, 'mydb.orders_archive', 'replace');  // drop + insert_many
```

**Write modes:**

| Database | `append` | `replace` | `upsert` |
|----------|----------|-----------|----------|
| PostgreSQL | INSERT (COPY for >10k rows) | — | ON CONFLICT DO UPDATE |
| MySQL | INSERT 1k/stmt (LOAD DATA for >10k) | REPLACE INTO | ON DUPLICATE KEY UPDATE |
| SQL Server | INSERT 500/stmt | INSERT 500/stmt | MERGE INTO |
| MongoDB | insert_many 10k/batch | drop + insert_many | — |

> **Note on SQL Server passwords:** Avoid using `!` in SQL Server passwords. A known issue in the tiberius v0.12 Rust driver causes authentication failure when `!` is present when connecting via TCP. Use only `[A-Za-z0-9_\-@#$]`. Fix planned for v0.3.2.

---

### 2. The Observer — Full DataFrame Export & EDA

```js
// Value frequency table
const stateCounts = df.valueCounts('state');
// { TX: 6345, CA: 6301, CO: 6304, ... }

// Unique values (insertion order)
const categories = df.unique('category');
// ['Electronics', 'Books', 'Clothing', ...]

// Full export to JavaScript
const records = df.toDict();    // Array of objects — all rows
const matrix  = df.toList();    // Array of arrays — values only
const jsonStr = df.toJson();    // JSON string "[{...}, ...]"
```

---

### 3. Native Math Foundation

Pure Rust arithmetic — no numeric libraries needed.

```js
// DataFrame arithmetic methods — return new DataFrame
const revenueDf = df.mul('price', 'quantity');   // result column: 'result_mul'
const profitDf  = df.sub('revenue', 'cost');     // result column: 'result_math_sub'
const totalDf   = df.add('amount', 'tax');       // result column: 'result_math_add'

// Standard deviation (scalar)
const stdVal = revenueDf.std('result_mul');
console.log(`Revenue std dev: ${stdVal.toFixed(2)}`);

// Min-Max normalization to [0, 1]
const normedDf = df.minMaxScale('price');        // result column: 'result_minmax'

// Sort
const sortedDf = df.sortValues('price', false);  // descending, CPU sort
```

---

### 4. GPU Sort — Bitonic Sort via WebGPU

```js
// GPU Bitonic sort — falls back to CPU silently if GPU unavailable
const sortedDf = df.sortValues('revenue', true, true);  // (by, ascending, gpu)
```

If a compatible GPU (Vulkan / Metal / DX12) is not available, PardoX automatically uses the parallel CPU sort. The result is identical either way.

---

### 5. Proxy-Based Column Syntax

PardoX wraps every DataFrame in a JavaScript `Proxy` so that column access and assignment feel native:

```js
// Column access → Series
const prices = df['price'];

// Arithmetic between Series → new Series
const revenue = df['price'].mul(df['quantity']);

// Column assignment from Series
df['revenue'] = df['price'].mul(df['quantity']);

// Filter using a boolean Series mask
const mask     = df['price'].gt(100.0);
const filtered = df.filter(mask);
```

---

## 📋 Full API Overview

### Top-level functions

```js
const {
    DataFrame,
    Series,
    read_csv,
    read_sql,    executeSql,
    read_mysql,  execute_mysql,
    read_sqlserver, execute_sqlserver,
    read_mongodb,   execute_mongodb,
    read_prdx,
} = require('@pardox/pardox');

const df = read_csv('./file.csv', { price: 'Float64' });  // optional schema
const df = read_sql(pgConn, 'SELECT * FROM orders');
const df = read_mysql(myConn, 'SELECT * FROM products');
const df = read_sqlserver(msConn, 'SELECT TOP 100 * FROM dbo.t');
const df = read_mongodb(mgConn, 'mydb.collection');

const rows = read_prdx('./file.prdx', 100);  // Array of objects (preview)

// Factory from in-memory JS array of objects
const df = DataFrame.fromArray([{ id: 1, val: 2.5 }, { id: 2, val: 3.1 }]);
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
df.toJson(50);            // JSON string, first 50 rows (preview)
```

### DataFrame — Arithmetic & Transform

```js
df.cast('quantity', 'Float64');
df.fillna(0.0);                        // returns this (chainable)
df.round(2);                           // returns this (chainable)
df.mul('price', 'quantity');           // → DataFrame with 'result_mul'
df.sub('revenue', 'cost');             // → DataFrame with 'result_math_sub'
df.add('amount', 'tax');               // → DataFrame with 'result_math_add'
df.std('column');                      // float
df.minMaxScale('col');                 // → DataFrame with 'result_minmax'
df.sortValues('col', true);            // → DataFrame (ascending)
df.sortValues('col', false, true);     // → DataFrame (descending, GPU)
```

### DataFrame — Join & Filter

```js
df.join(other, { on: 'id' });                          // hash join, same key name
df.join(other, { leftOn: 'order_id', rightOn: 'id' }); // different key names
df.filter(mask);                                        // → DataFrame
```

### Series — Column Access & Arithmetic

```js
const col  = df['price'];              // → Series
const rev  = df['price'].mul(df['quantity']);  // → Series

df['revenue'] = df['price'].mul(df['quantity']); // column assignment
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
df['price'].eq(100);      // ===
df['price'].neq(100);     // !==
df['price'].gt(100);      // >
df['price'].gte(100);     // >=
df['price'].lt(100);      // <
df['price'].lte(100);     // <=
```

### Observer

```js
df.valueCounts('col');   // { 'value': count, ... }
df.unique('col');        // ['val1', 'val2', ...]
df.toDict();             // [{ col: val, ... }, ...]  — all rows
df.toList();             // [[val, val, ...], ...]    — values only
df.toJson();             // JSON string — all rows
```

### Write

```js
df.toPrdx('./out.prdx');
df.toCsv('./out.csv');
df.toSql(pgConn, 'table', 'append', ['id']);
df.toMysql(myConn, 'table', 'upsert', ['id']);
df.toSqlserver(msConn, 'dbo.table', 'append');
df.toMongodb(mgConn, 'db.collection', 'append');
```

### IO — Standalone execute helpers

```js
executeSql(pgConn,       'CREATE TABLE ...');
execute_mysql(myConn,    'DROP TABLE IF EXISTS ...');
execute_sqlserver(msConn,'TRUNCATE TABLE dbo.staging');
execute_mongodb(mgConn,  'mydb', '{"drop": "col"}');
```

---

## 📊 Benchmarks

| Operation | Node.js (js-native) | PardoX v0.3.1 | Speedup |
|-----------|---------------------|---------------|---------|
| Read CSV (1 GB) | ~10s | ~0.8s | **~12x** |
| Column multiply (1M rows) | ~0.6s | ~0.02s | **~30x** |
| PostgreSQL write 50k rows | ~20s (pg execute) | ~0.6s (COPY) | **~33x** |
| MySQL write 50k rows | ~25s (mysql2) | ~3s (batch INSERT) | **~8x** |

---

## 🗺️ Roadmap

| Version | Status | Highlights |
|---------|--------|------------|
| v0.1 | ✅ Released | CSV, arithmetic, aggregations, .prdx format |
| v0.3.1 | ✅ Released | Databases (PG/MySQL/MSSQL/MongoDB), Observer, Math, GPU sort |
| v0.3.2 | 🔜 Planned | SQL Server `!` password fix, error hierarchy, GroupBy, Parquet reader |

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

[**Full Documentation →**](https://betoalien.github.io/PardoX/)

---

## 📄 License

MIT License — free for commercial and personal use.

---

<p align="center">by Alberto Cardenas<br>
<a href="https://www.albertocardenas.com">www.albertocardenas.com</a> · <a href="https://www.pardox.io">www.pardox.io</a></p>
