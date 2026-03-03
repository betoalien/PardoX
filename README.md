# PardoX — Hyper-Fast Data Engine

[![PyPI version](https://badge.fury.io/py/pardox.svg)](https://badge.fury.io/py/pardox)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Powered By Rust](https://img.shields.io/badge/powered%20by-Rust-orange.svg)](https://www.rust-lang.org/)
[![Version](https://img.shields.io/badge/version-0.3.1-blue.svg)]()

**The Speed of Rust. The Simplicity of Python.**

PardoX is a high-performance DataFrame engine for modern ETL and analytics. A **Rust** core powers SDKs in **Python, Node.js, and PHP**, with native database I/O and an ultra-fast binary format.

---

## ✅ Summary (v0.3.1)

- **Zero-Copy Architecture**: Rust buffers with no intermediate Python objects.
- **SIMD + Multithreading**: vectorized ops for 5x–20x speedups.
- **Native I/O**: PostgreSQL, MySQL, SQL Server, and MongoDB via Rust.
- **`.prdx` format**: ~4.6 GB/s read throughput for repeated workloads.
- **GPU Sort**: `sort_values(gpu=True)` with CPU fallback.
- **ML Ready**: zero-copy NumPy bridge via `__array__`.

---

## 📦 Quick Install

### Python

```bash
pip install pardox
```

### Node.js

```bash
npm install @pardox/pardox
```

### PHP

```bash
composer require betoalien/pardox-php
```

---

## 📚 Official Documentation

- Repository and docs: https://github.com/betoalien/PardoX
- New docs (Mintlify): https://betoalien-pardox.mintlify.app
- MkDocs documentation: https://betoalien.github.io/PardoX/

---

## 🧭 Community

- X (Twitter): https://x.com/pardox_io

---

## 📄 License

MIT
