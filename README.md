# PardoX — Hyper-Fast Data Engine

[![PyPI version](https://badge.fury.io/py/pardox.svg)](https://badge.fury.io/py/pardox)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Powered By Rust](https://img.shields.io/badge/powered%20by-Rust-orange.svg)](https://www.rust-lang.org/)
[![Version](https://img.shields.io/badge/version-0.3.4-blue.svg)]()

**The Speed of Rust. The Simplicity of Python.**

PardoX is a high-performance DataFrame engine for modern ETL and analytics. A **Rust** core powers SDKs in **Python, Node.js, and PHP**, with native database I/O, an ultra-fast binary format, and out-of-core processing for datasets larger than RAM.

> **v0.3.4 is now available.** SQL Cursor API (Gap 30) — streaming iterator over SQL results validated in Python, JS, PHP. Plus all 29 previous feature gaps: GroupBy, Window Functions, String & Date ops, Lazy Pipeline, Encryption, Data Contracts, Time Travel, Arrow Flight, Linear Algebra, REST Connector, Cloud Storage, PRDX Streaming to PostgreSQL (150M rows validated), and more.

---

## ✅ What's New in v0.3.4

| Feature | Status |
|---------|--------|
| **SQL Cursor API** (Gap 30) | `query_to_results(conn, query, batch_size)` — streaming iterator over SQL results with O(batch) RAM. `sql_to_parquet(conn, query, pattern, chunk_size)` — stream SQL → PardoX binary files. Validated: 3 SDKs × 11/11 tests. Requested by GitHub @Prussian1870 |

---

## ✅ What's New in v0.3.3

| Feature | Status |
|---------|--------|
| **SQL Cursor API** (Gap 30) — Rust Core | 5 new FFI exports: `pardox_scan_sql_cursor_open`, `pardox_scan_sql_cursor_fetch`, `pardox_scan_sql_cursor_offset`, `pardox_scan_sql_cursor_close`, `pardox_scan_sql_to_parquet`. SqlCursor struct with server-side PostgreSQL DECLARE cursor |

---

## ✅ What's New in v0.3.2

| Feature | Status |
|---------|--------|
| **PRDX Streaming to PostgreSQL** | Stream `.prdx` → PostgreSQL via `COPY FROM STDIN` with O(block) RAM. Validated: 150M rows / 3.8 GB in ~490s at ~306k rows/s |
| **GroupBy Aggregation** (Gap 1) | `df.groupby(col, {col: agg})` — sum, mean, count, min, max, std — Python, JS, PHP |
| **String & Date Operations** (Gap 2) | `str_upper`, `str_lower`, `str_contains`, `date_extract`, `date_diff`, `date_add` — all SDKs |
| **Decimal Type** (Gap 3) | Native Decimal128 column type with configurable precision and scale |
| **Window Functions** (Gap 4) | `row_number`, `rank`, `lag`, `lead`, `rolling_mean` — all SDKs |
| **Lazy Pipeline** (Gap 5) | `scan_csv().select().filter().limit().collect()` — all SDKs |
| **SQL over DataFrames** (Gap 14) | `df.sql("SELECT ... FROM df ...")` — all SDKs |
| **Out-of-Core Processing** (Gap 11) | `chunked_groupby`, `external_sort`, `spill_to_disk` — handles datasets > RAM |
| **Streaming GroupBy on .prdx** (Gap 13) | `prdx_groupby()` — O(groups) memory on any file size |
| **Encryption** (Gap 18) | `write_prdx_encrypted` / `read_prdx_encrypted` |
| **Data Contracts** (Gap 19) | `df.validate_contract(schema_json)` — row-level validation |
| **Time Travel** (Gap 20) | `version_write` / `version_read` / `version_list` — snapshot history |
| **Arrow Flight** (Gap 21) | `pardox_flight_start` / `pardox_flight_read` — high-throughput Arrow transport |
| **Linear Algebra** (Gap 28) | `cosine_sim`, `l2_normalize`, `matmul`, `pca` |
| **REST Connector** (Gap 29) | `read_rest(url, method, headers_json)` → DataFrame |
| **Cloud Storage** (Gap 15) | `read_cloud_csv` from S3, GCS, Azure |
| **30 Gaps Total** | All 30 feature gaps implemented in the Rust core across Python, JS, PHP |

---

## ✅ Core Capabilities (since v0.3.1)

- **Zero-Copy Architecture**: Rust HyperBlock buffers with no intermediate Python/JS/PHP objects.
- **SIMD + Multithreading**: AVX2/NEON vectorized ops for 5x–20x speedups.
- **Native Database I/O**: PostgreSQL, MySQL, SQL Server, MongoDB — no `psycopg2`, no `pymysql`.
- **`.prdx` format**: ~4.6 GB/s read throughput — faster than Parquet for repeated workloads.
- **GPU Sort**: `sort_values(gpu=True)` — WebGPU Bitonic sort with CPU fallback.
- **ML Ready**: Zero-copy NumPy bridge via `__array__` protocol.
- **Multi-SDK**: One Rust core, identical API in Python, Node.js, and PHP.

---

## 📦 Quick Install

### Python

```bash
pip install pardox
```

### Node.js

```bash
npm i @pardox/pardox
```

### PHP

```bash
composer require betoalien/pardox-php
```

---

## 🚀 Quick Start

```python
import pardox as px

# Load 100k rows — parallel Rust CSV parser
df = px.read_csv("sales.csv")
print(f"{df.shape[0]:,} rows × {df.shape[1]} columns")

# GroupBy — pure Rust
grouped = df.groupby("state", {"revenue": "sum", "qty": "count"})

# Stream 150M rows to PostgreSQL with O(block) RAM
rows = px.write_sql_prdx(
    "sales_150m.prdx",
    "postgresql://user:pass@localhost:5432/db",
    "sales", mode="append", conflict_cols=[], batch_rows=1_000_000
)

# Out-of-core: GroupBy on .prdx without loading all rows into RAM
result = px.prdx_groupby("sales_150m.prdx", ["region"], {"revenue": "sum"})
```

---

## 📊 Benchmarks (v0.3.4)

| Operation | Baseline | PardoX v0.3.2 | Speedup |
|-----------|----------|---------------|---------|
| Read CSV (1 GB) | Pandas ~4.2s | ~0.8s | **5x** |
| Column multiply (1M rows) | Pandas ~0.15s | ~0.02s | **7.5x** |
| PostgreSQL write 50k rows | psycopg2 ~18s | ~0.6s (COPY) | **30x** |
| MySQL write 50k rows | pymysql ~22s | ~3s (batch INSERT) | **7x** |
| PRDX -> PostgreSQL 150M rows | N/A | ~490s | 306k rows/s |

---

## 📚 Documentation

- Full docs: https://www.pardox.io
- Repository: https://github.com/betoalien/PardoX

---

## 🧭 Community

- X (Twitter): https://x.com/pardox_io

---

## 📄 License

MIT
