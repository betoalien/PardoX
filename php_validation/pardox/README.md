# PardoX for PHP — High-Performance DataFrame Engine

[![Packagist](https://img.shields.io/packagist/v/betoalien/pardox-php.svg)](https://packagist.org/packages/betoalien/pardox-php)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PHP 8.1+](https://img.shields.io/badge/php-8.1+-8892BF.svg)](https://www.php.net/)
[![Powered By Rust](https://img.shields.io/badge/powered%20by-Rust-orange.svg)](https://www.rust-lang.org/)

**The Speed of Rust. The Simplicity of PHP.**

PardoX is a high-performance DataFrame engine for PHP. A single **Rust core** handles all computation — CSV parsing, arithmetic, database I/O, sorting, GroupBy, Window functions, and more — exposed to PHP through the native FFI extension. No Python. No Node. No middleware.

> **v0.3.2 is now available.** PRDX streaming to PostgreSQL (150M rows validated), GroupBy, String & Date ops, Window functions, Lazy pipeline, SQL over DataFrames, Encryption, Data Contracts, Time Travel, Arrow Flight, Linear Algebra, REST Connector, and 29 total gap features — all from PHP.

---

## ⚡ Why PardoX for PHP?

| Capability | How |
|------------|-----|
| **Zero-copy ingestion** | Multi-threaded Rust CSV parser — no PHP string processing |
| **SIMD arithmetic** | AVX2 / NEON — 5x–20x faster than PHP loops |
| **Native database I/O** | Rust drivers for PostgreSQL, MySQL, SQL Server, MongoDB — no PHP extensions needed |
| **PRDX Streaming** | Stream 150M-row files to PostgreSQL at ~145k rows/s with O(block) RAM |
| **GPU sort** | WebGPU Bitonic sort with transparent CPU fallback |
| **GroupBy + Window** | Aggregations, rolling, rank, lag/lead — pure Rust |
| **No dependencies** | Only requires PHP's built-in `ext-ffi` and `ext-json` |
| **Cross-platform** | Linux x64 · Windows x64 · macOS Intel · macOS Apple Silicon |

---

## 📦 Installation

```bash
composer require betoalien/pardox-php
```

**Requirements:**
- PHP 8.1 or higher
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

// Load 100,000 rows — parallel Rust CSV parser
$df = DataFrame::read_csv('./sales_data.csv');
echo "Loaded " . number_format($df->shape[0]) . " rows\n";

// SIMD-accelerated arithmetic
$meanDiscount = $df->mean('discount');
echo sprintf("Mean discount: %.4f\n", $meanDiscount);

// Value frequency table
$stateCounts = $df->value_counts('state');
arsort($stateCounts);
foreach (array_slice($stateCounts, 0, 3, true) as $state => $count) {
    echo "  $state: $count\n";
}

// GroupBy aggregation
$grouped = $df->group_by('state', ['revenue' => 'sum', 'quantity' => 'mean']);

// Write to MySQL
$MYSQL = 'mysql://user:password@localhost:3306/mydb';
IO::executeMysql($MYSQL, 'CREATE TABLE IF NOT EXISTS sales (price DOUBLE, quantity DOUBLE)');
$rows = $df->to_mysql($MYSQL, 'sales', 'append');
echo "Written $rows rows to MySQL\n";
```

---

## 🗄️ What's New in v0.3.2

### PRDX Streaming to PostgreSQL

Stream a `.prdx` file directly to PostgreSQL — **without loading the file into RAM**. O(block) memory regardless of file size.

```php
use PardoX\IO;

$rows = IO::write_sql_prdx(
    '/data/ventas_150m.prdx',
    'postgresql://user:pass@localhost:5432/mydb',
    'ventas',           // table must already exist
    'append',
    [],
    1000000             // rows per COPY batch
);
echo "Streamed " . number_format($rows) . " rows\n";
// Validated: 150,000,000 rows / 3.8 GB in ~1,032s at ~145k rows/s
```

| Approach | RAM used |
|----------|----------|
| `read_prdx()` then `to_sql()` | Entire file (3.8 GB for 150M rows) |
| `IO::write_sql_prdx()` | O(one block) — typically < 200 MB |

---

### GroupBy Aggregation

```php
// Single aggregation
$grouped = $df->group_by('category', ['revenue' => 'sum']);

// Multiple aggregations
$grouped = $df->group_by('state', [
    'revenue'  => 'sum',
    'price'    => 'mean',
    'quantity' => 'count',
]);
```

---

### String & Date Operations

```php
// String ops
$df->str_upper('name');
$df->str_lower('email');
$df->str_trim('description');
$df->str_contains('tag', 'php');
$df->str_replace('status', 'old', 'new');

// Date ops
$df->date_extract('created_at', 'year');   // → 'result_year'
$df->date_format('created_at', '%Y-%m');
$df->date_diff('end_date', 'start_date');
$df->date_add('created_at', 30, 'day');
```

---

### Window Functions

```php
$df->row_number('price');
$df->rank('revenue', 'dense');
$df->lag('price', 1);
$df->lead('price', 1);
$df->rolling_mean('price', 7);   // 7-period moving average
```

---

### Lazy Pipeline

```php
use PardoX\IO;

// Scan without loading — filter and collect on demand
$result = IO::lazy_scan_csv('./large_file.csv')
    ->select(['id', 'price', 'state'])
    ->filter('price', '>', 100.0)
    ->limit(10000)
    ->collect();

echo $result->shape[0] . " rows\n";
```

---

### SQL over DataFrames

```php
// Register a DataFrame and run SQL against it
$df = DataFrame::read_csv('./orders.csv');
$result = $df->sql('SELECT state, SUM(revenue) as total FROM df GROUP BY state');
```

---

### Encryption

```php
use PardoX\IO;

// Write encrypted PRDX
IO::write_prdx_encrypted('./secure.prdx', $df, 'my-secret-key');

// Read back
$df = IO::read_prdx_encrypted('./secure.prdx', 'my-secret-key');
```

---

### Data Contracts

```php
$contract = json_encode([
    'source'  => 'orders',
    'columns' => [
        'price'    => ['min' => 0.0, 'max' => 10000.0],
        'status'   => ['allowed_values' => ['active', 'pending', 'closed']],
        'customer' => ['nullable' => false],
    ]
]);

// Returns a new DataFrame with only conforming rows
$clean = $df->validate_contract($contract);
$violations = $df->contract_violation_count();
echo "$violations rows quarantined\n";
```

---

## 🗄️ Database I/O

### 1. Relational Conqueror — PostgreSQL, MySQL, SQL Server, MongoDB

```php
use PardoX\DataFrame;
use PardoX\IO;

// ── PostgreSQL ────────────────────────────────────────────────
$PG = 'postgresql://user:pass@localhost:5432/mydb';

$df = IO::read_sql($PG, 'SELECT * FROM orders WHERE status = \'active\'');
IO::executeSql($PG, 'CREATE TABLE orders_archive (id BIGINT, amount FLOAT, region TEXT)');

// COPY FROM STDIN auto-activated for > 10,000 rows
$rows = $df->to_sql($PG, 'orders_archive', 'append');

// Upsert — INSERT … ON CONFLICT DO UPDATE
$rows = $df->to_sql($PG, 'orders_archive', 'upsert', ['id']);

// ── MySQL ─────────────────────────────────────────────────────
$MY = 'mysql://user:pass@localhost:3306/mydb';

$df = IO::read_mysql($MY, 'SELECT * FROM products WHERE active = 1');
$rows = $df->to_mysql($MY, 'products_bak', 'append');
$rows = $df->to_mysql($MY, 'products_bak', 'upsert', ['id']);

// ── SQL Server ────────────────────────────────────────────────
$MS = 'Server=localhost,1433;Database=mydb;UID=sa;PWD=MyPwd;TrustServerCertificate=Yes';

$df = IO::read_sqlserver($MS, 'SELECT TOP 5000 * FROM dbo.transactions');
$rows = $df->to_sqlserver($MS, 'dbo.transactions_bak', 'upsert', ['id']); // MERGE INTO

// ── MongoDB ───────────────────────────────────────────────────
$MG = 'mongodb://admin:pass@localhost:27017';

$df = IO::read_mongodb($MG, 'mydb.orders');
$rows = $df->to_mongodb($MG, 'mydb.orders_archive', 'append');
```

**Write modes:**

| Database | `append` | `replace` | `upsert` |
|----------|----------|-----------|----------|
| PostgreSQL | INSERT (COPY for >10k rows) | — | ON CONFLICT DO UPDATE |
| MySQL | INSERT 1k/stmt (LOAD DATA for >10k) | REPLACE INTO | ON DUPLICATE KEY UPDATE |
| SQL Server | INSERT 500/stmt | INSERT 500/stmt | MERGE INTO |
| MongoDB | insert_many 10k/batch | drop + insert_many | — |

> **Note on SQL Server passwords:** A known issue in the tiberius v0.12 Rust driver causes authentication failure when `!` is present in the password. Use only `[A-Za-z0-9_\-@#$]`. Fix planned for v0.4.0.

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
$df->mul('price', 'quantity');       // → DataFrame with 'result_mul'
$df->sub('revenue', 'cost');         // → DataFrame with 'result_sub'
$df->add('amount', 'tax');           // → DataFrame with 'result_add'
$df->std('column');                  // float
$df->min_max_scale('col');           // → DataFrame with 'result_minmax'
$df->sort_values('col', true);       // → DataFrame (ascending)
$df->sort_values('col', false, true);// → DataFrame (descending, GPU)
```

### DataFrame — GroupBy & Aggregation

```php
$df->group_by('category', ['revenue' => 'sum', 'price' => 'mean']);
$df->group_by('state', ['quantity' => 'count', 'revenue' => 'max']);
```

### DataFrame — Window Functions

```php
$df->row_number('price');
$df->rank('revenue', 'dense');
$df->lag('price', 1);
$df->lead('price', 1);
$df->rolling_mean('price', 7);
```

### DataFrame — String Operations

```php
$df->str_upper('col');
$df->str_lower('col');
$df->str_trim('col');
$df->str_len('col');
$df->str_contains('col', 'pattern');
$df->str_replace('col', 'old', 'new');
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
IO::write_sql_prdx('file.prdx', $pgConn, 'table', 'append', [], 1000000);
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

| Operation | PHP PDO / loops | PardoX v0.3.2 | Speedup |
|-----------|----------------|---------------|---------|
| Read CSV (1 GB) | ~12s | ~0.8s | **~15x** |
| Column multiply (1M rows) | ~0.8s | ~0.02s | **~40x** |
| PostgreSQL write 50k rows | ~20s (PDO execute) | ~0.6s (COPY) | **~33x** |
| MySQL write 50k rows | ~25s (PDO execute) | ~3s (batch INSERT) | **~8x** |
| PRDX → PostgreSQL 150M rows | N/A | ~1,032s | 145k rows/s |

---

## 🗺️ Roadmap

| Version | Status | Highlights |
|---------|--------|------------|
| v0.1 | ✅ Released | CSV, arithmetic, aggregations, .prdx format |
| v0.3.1 | ✅ Released | Databases (PG/MySQL/MSSQL/MongoDB), Observer, Math, GPU sort |
| v0.3.2 | ✅ Released | PRDX Streaming, GroupBy, Window, String/Date, Lazy, Encryption, Data Contracts, Time Travel, Arrow Flight, Linear Algebra, REST Connector — 29 features |
| v0.4.0 | 🔜 Planned | SQL Server `!` password fix, structured error codes, Apache Parquet, Kafka, S3 |

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
