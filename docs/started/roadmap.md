---
layout: default
title: "Roadmap"
parent: "Getting Started"
nav_order: 3
---

# Roadmap

This page tracks features planned for upcoming releases and the reasoning behind them.

---

## v0.3.1 — Released (2026-02-28)

All items below were delivered in this release.

| Feature | Status |
|---------|--------|
| PostgreSQL read / write / execute | ✅ Done |
| MySQL read / write / execute | ✅ Done |
| SQL Server read / write / execute | ✅ Done |
| MongoDB read / write / execute | ✅ Done |
| PostgreSQL COPY FROM STDIN (> 10k rows) | ✅ Done |
| MySQL LOAD DATA LOCAL INFILE (> 10k rows) | ✅ Done (server-side flag required) |
| SQL Server batch INSERT 500 rows/stmt | ✅ Done |
| SQL Server MERGE upsert | ✅ Done |
| MongoDB ordered:false, 10k doc batches | ✅ Done |
| `to_dict()`, `to_json()` — full DataFrame export | ✅ Done |
| `value_counts()`, `unique()` — EDA Observer | ✅ Done |
| `df.add()`, `df.sub()`, `df.std()`, `df.min_max_scale()` | ✅ Done |
| `df.sort_values(ascending, gpu)` | ✅ Done |
| GPU Bitonic sort (WebGPU / wgpu) with CPU fallback | ✅ Done |
| Zero-copy NumPy bridge (`__array__` protocol) | ✅ Done |
| PHP SDK — full feature parity | ✅ Done |
| Node.js SDK — full feature parity | ✅ Done |
| FFI memory safety — heap vs. thread-local string ownership | ✅ Fixed |

---

## v0.3.2 — Released (2026-03-15)

All items below were delivered in this release.

| Feature | Status |
|---------|--------|
| `write_sql_prdx()` — PRDX streaming to PostgreSQL via COPY FROM STDIN, O(block) RAM | ✅ Done |
| GroupBy aggregation (Gap 1) | ✅ Done — Python, JS, PHP |
| String & Date operations (Gap 2) | ✅ Done — Python, JS, PHP |
| Decimal type (Gap 3) | ✅ Done — Python, JS, PHP |
| Window functions (Gap 4) | ✅ Done — Python, JS, PHP |
| Lazy pipeline (Gap 5) | ✅ Done — Python, JS, PHP |
| GPU compute (Gap 7) | ✅ Done — Python, PHP |
| Pivot & Melt (Gap 8) | ✅ Done — Python, PHP |
| Time Series Fill (Gap 9) | ✅ Done — Python, PHP |
| Nested Data / JSON (Gap 10) | ✅ Done — Python, PHP |
| Spill to Disk (Gap 11) | ✅ Done — Python, PHP |
| Universal Loader PRDX (Gap 12) | ✅ Done — Python, PHP |
| Streaming GroupBy (Gap 13) | ✅ Done — PHP |
| SQL over DataFrames (Gap 14) | ✅ Done — Python, JS, PHP |
| Cloud Storage (Gap 15) | ✅ Done — Python |
| Live Query (Gap 16) | ✅ Done — Python |
| WebAssembly (Gap 17) | ✅ Done — JS |
| Encryption (Gap 18) | ✅ Done — Python |
| Data Contracts (Gap 19) | ✅ Done — Python |
| Time Travel (Gap 20) | ✅ Done — Python |
| Arrow Flight (Gap 21) | ✅ Done — Python |
| Distributed Cluster (Gap 22) | ✅ Done — Python |
| SQL Server Fix (Gap 23) | ✅ Done — Python |
| Fault Tolerance (Gap 26) | ✅ Done — Python |
| Query Planner (Gap 27) | ✅ Done — Python |
| Linear Algebra (Gap 28) | ✅ Done — Python |
| REST Connector (Gap 29) | ✅ Done — Python |
| FFI Reference documentation (181 exports) | ✅ Done |
| VAP31 CSV→PostgreSQL — 3 SDKs | ✅ Done |
| VAP32 PRDX→PostgreSQL — 3 SDKs | ✅ Done — 150M rows validated |

---

## v0.4.0 — Planned

### SQL Server — Password Special Character Fix

**Problem:** The tiberius Rust driver (v0.12) fails to authenticate when the SQL Server password contains `!` or certain other special characters.

**Planned fix:** Audit `tiberius` v0.12.x `login.rs` and patch or upgrade to a version where the encoding is correct for all printable ASCII characters.

**Workaround until fixed:** Use a SQL Server password that contains only `[A-Za-z0-9_\-@#$]`.

---

### Error Code Hierarchy

**Problem:** All database errors currently return `-100` regardless of the actual failure reason, making diagnostics difficult.

**Planned improvement:**

| Code range | Category |
|------------|----------|
| -100 to -199 | Connection / authentication errors |
| -200 to -299 | Query parse / syntax errors |
| -300 to -399 | Data type mismatch |
| -400 to -499 | Constraint violations |
| -500 to -599 | Timeout errors |

---

### Additional Native Data Sources

| Source | Format / Protocol | Notes |
|--------|------------------|-------|
| Apache Parquet | Binary columnar | Direct read into HyperBlock via `parquet2` crate |
| Apache Kafka | Streaming | Topic → DataFrame streaming ingestion |
| AWS S3 | Object storage | CSV / Parquet over S3 via `aws-sdk-rust` |
| Snowflake | JDBC-like | Via Snowflake Arrow export |
| Oracle DB | OCI | Via `oracle` Rust crate |
| SQLite | File-based SQL | Via `rusqlite` |

---

### Gaps 7–14 — JavaScript SDK

JavaScript SDK currently segfaults on Gaps 7–11 due to a known Core issue. Fix and re-validate planned.

---

## Long-Term Vision

- **Distributed execution:** Partition HyperBlocks across nodes via a Rust-native scheduler.
- **Streaming DataFrames:** Ingest from Kafka / Kinesis as append-only HyperBlock partitions.
- **GPU-accelerated aggregations:** Extend GPU support from sort to sum, mean, and groupby using compute shaders.
- **WASM SDK:** Full PardoX in the browser via WebAssembly.
