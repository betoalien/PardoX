# PardoX for PHP — High-Performance DataFrame Engine

[![Packagist](https://img.shields.io/packagist/v/pardox/pardox.svg)](https://packagist.org/packages/pardox/pardox)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PHP 7.4+](https://img.shields.io/badge/php-7.4+-8892BF.svg)](https://www.php.net/)
[![Powered By Rust](https://img.shields.io/badge/powered%20by-Rust-orange.svg)](https://www.rust-lang.org/)

**The Speed of Rust. The Simplicity of PHP.**

PardoX is a high-performance DataFrame engine for PHP. A single **Rust core** handles all computation — CSV parsing, arithmetic, database I/O, and sorting — exposed to PHP through the native FFI extension. No Python. No Node. No middleware.

> **v0.3.1 is now available.** Native database connectivity for PostgreSQL, MySQL, SQL Server, and MongoDB. Full Observer export. GPU sort. All from PHP.

---

## ⚡ Why PardoX for PHP?

| Capability | How |
|------------|-----|
| **Zero-copy ingestion** | Multi-threaded Rust CSV parser — no PHP string processing |
| **SIMD arithmetic** | AVX2 / NEON — 5x–20x faster than PHP loops |
| **Native database I/O** | Rust drivers for PostgreSQL, MySQL, SQL Server, MongoDB — no PHP extensions needed |
| **GPU sort** | WebGPU Bitonic sort with transparent CPU fallback |
| **No dependencies** | Only requires PHP's built-in `ext-ffi` and `ext-json` |
| **Cross-platform** | Linux x64 · Windows x64 · macOS Intel · macOS Apple Silicon |

---

## 📦 Installation

```bash
composer require pardox/pardox
```

**Requirements:**
- PHP 7.4 or higher
- `ext-ffi` enabled in `php.ini`
- `ext-json` (standard, usually enabled by default)

Enable the FFI extension:

```ini
; php.ini
extension=ffi
ffi.enable=true
```

---

## 🚀 Quick Start

```php
<?php
require_once 'vendor/autoload.php';

use PardoX\DataFrame;
use PardoX\IO;

// Load 50,000 rows — parallel Rust CSV parser
$df = DataFrame::read_csv('./sales_data.csv');
echo "Loaded " . number_format($df->shape[0]) . " rows\n";

// SIMD-accelerated arithmetic
$meanDiscount = $df->mean('discount');
echo sprintf("Mean discount: %.4f\n", $meanDiscount);

// Value frequency table
$stateCounts = $df->value_counts('state');
echo "Unique states: " . count($stateCounts) . "\n";
arsort($stateCounts);
foreach (array_slice($stateCounts, 0, 3, true) as $state => $count) {
    echo "  $state: $count\n";
}

// Write to MySQL — chunked batch INSERT, auto LOAD DATA if server allows
$MYSQL = 'mysql://user:password@localhost:3306/mydb';
IO::executeMysql($MYSQL, 'CREATE TABLE IF NOT EXISTS sales (price DOUBLE, quantity DOUBLE)');
$rows = $df->to_mysql($MYSQL, 'sales', 'append');
echo "Written $rows rows to MySQL\n";
```

---

## 🗄️ What's New in v0.3.1

### 1. Relational Conqueror — Native Database I/O

Connect to **PostgreSQL, MySQL, SQL Server, and MongoDB** entirely through the Rust core. No `PDO`, no `mysqli`, no MongoDB PHP extension required.

```php
use PardoX\DataFrame;
use PardoX\IO;

// ── PostgreSQL ────────────────────────────────────────────────
$PG = 'postgresql://user:pass@localhost:5432/mydb';

$df = IO::read_sql($PG, 'SELECT * FROM orders WHERE status = \'active\'');
IO::executeSql($PG, 'DROP TABLE IF EXISTS orders_archive');
IO::executeSql($PG, 'CREATE TABLE orders_archive (id BIGINT, amount FLOAT, region TEXT)');

// COPY FROM STDIN auto-activated for > 10,000 rows
$rows = $df->to_sql($PG, 'orders_archive', 'append');

// Upsert — INSERT … ON CONFLICT DO UPDATE
$rows = $df->to_sql($PG, 'orders_archive', 'upsert', ['id']);

// ── MySQL ─────────────────────────────────────────────────────
$MY = 'mysql://user:pass@localhost:3306/mydb';

$df = IO::read_mysql($MY, 'SELECT * FROM products WHERE active = 1');
IO::executeMysql($MY, 'CREATE TABLE IF NOT EXISTS products_bak (id BIGINT, price DOUBLE)');

$rows = $df->to_mysql($MY, 'products_bak', 'append');            // chunked INSERT 1k/stmt
$rows = $df->to_mysql($MY, 'products_bak', 'replace');           // REPLACE INTO
$rows = $df->to_mysql($MY, 'products_bak', 'upsert', ['id']);    // ON DUPLICATE KEY UPDATE

// ── SQL Server ────────────────────────────────────────────────
$MS = 'Server=localhost,1433;Database=mydb;UID=sa;PWD=MyPwd;TrustServerCertificate=Yes';

$df = IO::read_sqlserver($MS, 'SELECT TOP 5000 * FROM dbo.transactions');
IO::executeSqlserver($MS, 'DROP TABLE IF EXISTS dbo.transactions_bak');

$rows = $df->to_sqlserver($MS, 'dbo.transactions_bak', 'append'); // 500 rows/stmt batch INSERT
$rows = $df->to_sqlserver($MS, 'dbo.transactions_bak', 'upsert', ['id']); // MERGE INTO

// ── MongoDB ───────────────────────────────────────────────────
$MG = 'mongodb://admin:pass@localhost:27017';

$df = IO::read_mongodb($MG, 'mydb.orders');
IO::executeMongodb($MG, 'mydb', '{"drop": "orders_archive"}');

$rows = $df->to_mongodb($MG, 'mydb.orders_archive', 'append');   // 10k docs/batch, ordered:false
$rows = $df->to_mongodb($MG, 'mydb.orders_archive', 'replace');  // drop + insert
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

```php
// Value frequency table
$stateCounts = $df->value_counts('state');
// ['TX' => 6345, 'CA' => 6301, ...]

// Unique values
$categories = $df->unique('category');
// ['Electronics', 'Books', ...]

// Full export
$records = $df->to_dict();   // array of associative arrays
$json    = $df->to_json();   // JSON string "[{...}, ...]"
$matrix  = $df->tolist();    // array of arrays (values only)
```

---

### 3. Native Math Foundation

```php
// DataFrame arithmetic methods — return new DataFrame
$revenueDf = $df->mul('price', 'quantity');   // result column: 'result_mul'
$profitDf  = $df->sub('revenue', 'cost');     // result column: 'result_sub'
$totalDf   = $df->add('amount', 'tax');       // result column: 'result_add'

// Standard deviation (scalar)
$std = $revenueDf->std('result_mul');
echo sprintf("Revenue std dev: %.2f\n", $std);

// Min-Max normalization to [0, 1]
$normedDf = $df->min_max_scale('price');      // result column: 'result_minmax'

// Sort
$sorted = $df->sort_values('price', false);   // descending, CPU sort
```

---

### 4. GPU Sort

```php
// GPU Bitonic sort — falls back to CPU silently if GPU unavailable
$sorted = $df->sort_values('revenue', true, true);  // ($by, $ascending, $gpu)
```

---

## 📋 Full API Overview

### DataFrame — Factory Methods

```php
$df = DataFrame::read_csv('./file.csv');
$df = DataFrame::read_csv('./file.csv', ['price' => 'Float64']);  // with schema
$df = IO::read_sql($pgConn, 'SELECT * FROM orders');
$df = IO::read_mysql($myConn, 'SELECT * FROM products');
$df = IO::read_sqlserver($msConn, 'SELECT TOP 100 * FROM dbo.t');
$df = IO::read_mongodb($mgConn, 'mydb.collection');
```

### DataFrame — Properties

```php
$df->shape      // [rows, cols]
$df->columns    // ['col1', 'col2', ...]
$df->dtypes     // ['col1' => 'Float64', ...]
```

### DataFrame — Inspection

```php
$df->show(10);              // ASCII table (stdout)
$df->head(5);               // → DataFrame
$df->tail(5);               // → DataFrame
$df->iloc(0, 100);          // → DataFrame (rows 0–99)
```

### DataFrame — Arithmetic & Transform

```php
$df->cast('quantity', 'Float64');
$df->fillna(0.0);
$df->round(2);
$df->mul('price', 'quantity');   // → DataFrame with 'result_mul'
$df->sub('revenue', 'cost');     // → DataFrame with 'result_sub'
$df->add('amount', 'tax');       // → DataFrame with 'result_add'
$df->std('column');              // float
$df->min_max_scale('col');       // → DataFrame with 'result_minmax'
$df->sort_values('col', true);   // → DataFrame (ascending)
$df->sort_values('col', false, true);  // → DataFrame (descending, GPU)
```

### DataFrame — Series Operators

```php
$total   = $df['price'] * $df['quantity'];    // → Series
$net     = $df['revenue'] - $df['discount'];  // → Series
$df['total'] = $df['price'] * $df['quantity']; // assign new column
```

### Series — Aggregations

```php
$df['col']->sum();     // float
$df['col']->mean();    // float
$df['col']->min();     // float
$df['col']->max();     // float
$df['col']->std();     // float
$df['col']->count();   // int
```

### Observer

```php
$df->value_counts('col');   // ['value' => count, ...]
$df->unique('col');         // ['val1', 'val2', ...]
$df->to_dict();             // [['col' => val, ...], ...]
$df->to_json();             // string
$df->tolist();              // [[val, val, ...], ...]
```

### Write

```php
$df->to_prdx('./out.prdx');
$df->to_csv('./out.csv');
$df->to_sql($pgConn, 'table', 'append', ['id']);
$df->to_mysql($myConn, 'table', 'upsert', ['id']);
$df->to_sqlserver($msConn, 'dbo.table', 'append');
$df->to_mongodb($mgConn, 'db.collection', 'append');
```

### IO — Static Helpers

```php
IO::executeSql($pgConn, 'CREATE TABLE ...');
IO::executeMysql($myConn, 'DROP TABLE IF EXISTS ...');
IO::executeSqlserver($msConn, 'TRUNCATE TABLE dbo.staging');
IO::executeMongodb($mgConn, 'mydb', '{"drop": "col"}');
```

---

## 📊 Benchmarks

| Operation | PHP PDO | PardoX v0.3.1 | Speedup |
|-----------|---------|---------------|---------|
| Read CSV (1 GB) | ~12s | ~0.8s | **~15x** |
| Column multiply (1M rows) | ~0.8s | ~0.02s | **~40x** |
| PostgreSQL write 50k rows | ~20s (PDO execute) | ~0.6s (COPY) | **~33x** |
| MySQL write 50k rows | ~25s (PDO execute) | ~3s (batch INSERT) | **~8x** |

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

[**Full Documentation →**](https://www.pardox.io)

---

## 📄 License

MIT License — free for commercial and personal use.

---

<p align="center">by Alberto Cardenas<br>
<a href="https://www.albertocardenas.com">www.albertocardenas.com</a> · <a href="https://www.pardox.io">www.pardox.io</a></p>
