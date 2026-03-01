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

## v0.3.2 — Planned

### SQL Server — Password Special Character Fix

**Problem:** The tiberius Rust driver (v0.12) fails to authenticate when the SQL Server password contains `!` or certain other special characters. The connection succeeds from the local machine (loopback), but fails from an external host. Root cause is in the `unicode_password()` XOR encoding function inside the TDS `LOGIN7` packet.

**Planned fix:** Audit `tiberius` v0.12.x `login.rs` — specifically the `unicode_password()` function — and patch or upgrade to a version where the encoding is correct for all printable ASCII characters.

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

The following connectors are planned for native Rust implementation (no Python driver dependency):

| Source | Format / Protocol | Notes |
|--------|------------------|-------|
| Apache Parquet | Binary columnar | Direct read into HyperBlock via `parquet2` crate |
| Apache Kafka | Streaming | Topic → DataFrame streaming ingestion |
| AWS S3 | Object storage | CSV / Parquet over S3 via `aws-sdk-rust` |
| Snowflake | JDBC-like | Via Snowflake Arrow export |
| Oracle DB | OCI | Via `oracle` Rust crate |
| SQLite | File-based SQL | Via `rusqlite` |

---

### Fake PostgreSQL Server (Validation Tool)

**Purpose:** Enable SDK validation tests to run without a real PostgreSQL instance. A lightweight Rust server implementing a subset of the PostgreSQL wire protocol will respond to `SELECT`, `INSERT`, `COPY`, and DDL statements, returning predictable results.

**Use cases:**
- CI/CD pipeline tests that cannot provision real databases
- SDK integration tests against a controlled server
- Protocol-level regression testing for the `pardox_write_sql` COPY path

---

### GroupBy / Split-Apply-Combine

Vectorized `groupby` with Rust hash-aggregation engine.

```python
# Coming in v0.3.2
summary = df.groupby("region").agg({"revenue": "sum", "quantity": "mean"})
```

---

### DataFrame Filtering DSL

A fluent filter API backed by Rust predicate evaluation.

```python
# Coming in v0.3.2
df_filtered = df.filter(df["price"] > 100).filter(df["state"] == "TX")
```

---

## Long-Term Vision

- **Distributed execution:** Partition HyperBlocks across nodes via a Rust-native scheduler.
- **Streaming DataFrames:** Ingest from Kafka / Kinesis as append-only HyperBlock partitions.
- **GPU-accelerated aggregations:** Extend GPU support from sort to sum, mean, and groupby using compute shaders.
- **WASM SDK:** PardoX in the browser via WebAssembly.
