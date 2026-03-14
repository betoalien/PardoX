---
layout: default
title: Home
nav_order: 1
---

# Welcome to PardoX

[![PyPI version](https://badge.fury.io/py/pardox.svg)](https://badge.fury.io/py/pardox)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Powered By Rust](https://img.shields.io/badge/powered%20by-Rust-orange.svg)](https://www.rust-lang.org/)
[![Version](https://img.shields.io/badge/version-0.3.1-blue.svg)]()

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
- [**Roadmap**](started/roadmap.md) — What's coming in v0.3.2 and beyond.

### 📘 User Guide
- [**Input / Output**](guide/io.md) — Multi-threaded CSV, native .prdx format, Apache Arrow bridge.
- [**Databases**](guide/databases.md) — PostgreSQL, MySQL, SQL Server, MongoDB — read, write, and execute.
- [**Data Mutation**](guide/mutation.md) — Vectorized arithmetic, type casting, sorting, data cleaning.
- [**Aggregations & Observer**](guide/aggregations.md) — Metrics, statistics, value counts, and full-DataFrame export.
- [**GPU Acceleration**](guide/gpu.md) — GPU Bitonic sort and CPU fallback.
- [**ML Integration**](guide/ml.md) — Zero-copy NumPy bridge and Scikit-Learn compatibility.

### ⚙️ API Reference
- [**Full Reference**](reference.md) — Detailed documentation of all classes, functions, and methods.

### 📓 Examples & Notebooks
- [**Jupyter Notebooks**](https://github.com/betoalien/PardoX/tree/master/notebooks) — Interactive examples and real-world ETL scenarios.
- [**Benchmark Scripts**](https://github.com/betoalien/PardoX/tree/master/python%20files) — Processing 640 million rows and transforming data to `.prdx` format.

---

## 📦 Quick Install

```bash
pip install pardox
```

---

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
