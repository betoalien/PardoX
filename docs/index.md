---
layout: default
title: Home
nav_order: 1
---

# Welcome to PardoX

[![PyPI version](https://badge.fury.io/py/pardox.svg)](https://badge.fury.io/py/pardox)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Powered By Rust](https://img.shields.io/badge/powered%20by-Rust-orange.svg)](https://www.rust-lang.org/)
[![Version](https://img.shields.io/badge/version-0.3.2-blue.svg)]()

**The Speed of Rust. The Simplicity of Python.**

PardoX is a high-performance DataFrame engine designed for modern data engineering. It combines the safety and speed of a **Rust Core** with the ease of use of Python, PHP, and Node.js SDKs — allowing you to process massive datasets and integrate with any database without learning a new language.

---

## 🚀 Why PardoX?

- **Zero-Copy Architecture:** Data flows directly from disk or database into memory-mapped Rust buffers — no Python objects, no intermediate copies.
- **SIMD Acceleration:** Mathematical operations use AVX2/NEON CPU instructions for 5x–20x speedups vs. Python loops.
- **Universal Compatibility:** Runs natively on Windows, Linux, and macOS (Intel & Apple Silicon).
- **Native Database Engine:** Connect to PostgreSQL, MySQL, SQL Server, and MongoDB entirely through Rust — no Python database drivers required.
- **Multi-SDK:** A single Rust core powers identical APIs in Python, Node.js, and PHP.
- **Native Format:** The `.prdx` binary format enables ~4.6 GB/s read throughput for repeated workloads.

---

## 📚 Documentation

### 🏁 Getting Started
- [**Installation**](started/installation.md) — Setup guide for Python, Node.js, and PHP.
- [**Quick Start**](started/quickstart.md) — Build your first ETL pipeline in 5 minutes.
- [**Roadmap**](started/roadmap.md) — What's coming in v0.4 and beyond.

### 📘 User Guide
- [**Input / Output**](guide/io.md) — Multi-threaded CSV, native .prdx format, Apache Arrow bridge.
- [**Databases**](guide/databases.md) — PostgreSQL, MySQL, SQL Server, MongoDB — read, write, and execute.
- [**Data Mutation**](guide/mutation.md) — Vectorized arithmetic, type casting, sorting, data cleaning.
- [**Aggregations & Observer**](guide/aggregations.md) — Metrics, statistics, value counts, and full-DataFrame export.
- [**GPU Acceleration**](guide/gpu.md) — GPU Bitonic sort and CPU fallback.
- [**ML Integration**](guide/ml.md) — Zero-copy NumPy bridge and Scikit-Learn compatibility.

### ⚙️ API Reference
- [**Full Reference**](reference.md) — Detailed documentation of all classes, functions, and methods.
- [**FFI Exports Reference**](ffi-reference.md) — All 181 C-ABI functions exported by the Rust core across 5 crates. Use this to build custom bindings or validate SDK integrations.

### 📂 Base Knowledge
- [**Base Knowledge**](base_knowledge/) — Validation scripts for all 29 feature gaps across Python, Node.js, and PHP SDKs.
  - [Python validation scripts](base_knowledge/python_code/) — 28 files (`validate_gap1_sdk.py` → `validate_gap29_sdk.py`)
  - [Node.js validation scripts](base_knowledge/js_code/) — 18 files (`validate_gap1_sdk.js` → `validate_gap18_sdk.js`)
  - [PHP validation scripts](base_knowledge/php_code/) — 28 files (`validate_gap1_sdk.php` → `validate_gap29_sdk.php`)

### 📘 SDK Documentation
- [**SDK Documentation**](documentation_sdk/) — In-depth guides for each SDK.
  - [Python SDK](documentation_sdk/python_sdk.md) — API reference and v0.3.2 features
  - [Node.js SDK](documentation_sdk/javascript_sdk.md) — API reference and v0.3.2 features
  - [PHP SDK](documentation_sdk/php_sdk.md) — API reference and v0.3.2 features
  - [Database Integration](documentation_sdk/db_integration.md) — PostgreSQL, MySQL, SQL Server, MongoDB across all SDKs
  - [Universality](documentation_sdk/universalidad.md) — Cross-SDK design philosophy

### 📓 Examples & Notebooks
- [**Jupyter Notebooks**](https://github.com/betoalien/PardoX/tree/master/notebooks) — Interactive examples and real-world ETL scenarios.
- [**Benchmark Scripts**](https://github.com/betoalien/PardoX/tree/master/python%20files) — Processing 640 million rows and transforming data to `.prdx` format.

---

## 📦 Quick Install

```bash
pip install pardox
```

---

## What's New in v0.3.2

| Pillar | What was added |
|--------|----------------|
| **PRDX Streaming to PostgreSQL** | `write_sql_prdx()` — stream any `.prdx` file directly to PostgreSQL via `COPY FROM STDIN` with O(block) RAM. Validated: 150M rows / 3.8 GB in ~490s at ~300k rows/s (Python/JS) |
| **Gaps 1–5 — All SDKs** | GroupBy, String & Date ops, Decimal type, Window functions, Lazy pipeline — validated across Python, JavaScript, and PHP SDKs |
| **Gaps 7–14 — Python** | GPU compute, Pivot & Melt, Time Series Fill, Nested Data (JSON), Spill to Disk, Universal Loader (PRDX), SQL over DataFrames |
| **Gaps 15–29 — Python** | Cloud Storage, Live Query, WebAssembly, Encryption, Data Contracts, Time Travel, Arrow Flight, Distributed Cluster, Linear Algebra, REST Connector |
| **VAP31 & VAP32** | CSV→PostgreSQL and PRDX→PostgreSQL integrations validated in 3 SDKs |
| **29 Gaps Total** | All 29 feature gaps from the original roadmap implemented in the Rust core |
| **FFI Reference** | Complete documentation of all 181 C-ABI exports across 5 crates |

## What's New in v0.3.1

| Pillar | What was added |
|--------|----------------|
| **Relational Conqueror** | Native read/write/execute for PostgreSQL, MySQL, SQL Server, MongoDB via Rust drivers |
| **The Observer** | `to_dict()`, `to_json()`, `value_counts()`, `unique()` — full-DataFrame export with proper heap memory management |
| **Native Math** | `df.add()`, `df.sub()`, `df.std()`, `df.min_max_scale()`, `df.sort_values()` — pure Rust arithmetic |
| **GPU Awakening** | `sort_values(gpu=True)` — WebGPU Bitonic sort with automatic CPU fallback |
| **ML Integration** | Zero-copy NumPy bridge via `__array__` protocol — direct pointer into Rust buffer |
| **PHP & Node.js SDKs** | Full parity with Python SDK across all new features |

---

<p align="center"><small>Open Source Project distributed under the MIT License.</small></p>
<p align="center">More info: <a href="https://www.pardox.io">www.pardox.io</a></p>
