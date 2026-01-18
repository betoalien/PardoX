# Welcome to PardoX

[![PyPI version](https://badge.fury.io/py/pardox.svg)](https://badge.fury.io/py/pardox)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Powered By Rust](https://img.shields.io/badge/powered%20by-Rust-orange.svg)](https://www.rust-lang.org/)

**The Speed of Rust. The Simplicity of Python.**

PardoX is a high-performance DataFrame engine designed for modern data engineering. It combines the safety and speed of a **Rust Core** with the ease of use of a **Python SDK**, allowing you to process massive datasets efficiently without learning a new language.

---

## 🚀 Why PardoX?

* **Zero-Copy Architecture:** Data is loaded directly into memory-mapped buffers.
* **SIMD Acceleration:** Mathematical operations utilize AVX2/NEON CPU instructions.
* **Universal Compatibility:** Runs natively on Windows, Linux, and MacOS (Intel & Apple Silicon).
* **Native Format:** The `.prdx` binary format allows for instant data persistence.

---

## 📚 Documentation Modules

Select a topic to start building faster pipelines:

### 🏁 Getting Started
* [**Installation**](started/installation.md) - Setup guide for Windows, Linux, and Mac.
* [**Quick Start**](started/quickstart.md) - Build your first ETL pipeline in 5 minutes.

### 📘 User Guide
* [**Input / Output**](guide/io.md) - Learn about the multi-threaded CSV reader and SQL engine.
* [**Data Mutation**](guide/mutation.md) - Perform vectorized arithmetic and data cleaning.
* [**Aggregations**](guide/aggregations.md) - Extract business insights and statistical metrics.

### ⚙️ API Reference
* [**Full Reference**](reference.md) - Detailed documentation of classes and functions.

### 📓 Examples & Notebooks
* [**Jupyter Notebooks**](https://github.com/betoalien/PardoX/tree/master/notebooks) - Interactive examples and tutorials showcasing PardoX capabilities, including the v0.1 Beta Showcase with real-world ETL scenarios.
* [**Benchmark Scripts**](https://github.com/betoalien/PardoX/tree/master/python%20files) - Production-ready example for processing 640 million rows and transforming data into `.prdx` format, demonstrating real-world performance at scale.

---

## 📦 Installation

Get started immediately via pip:

```bash
pip install pardox
```

<p align="center"> <small>Open Source Project distributed under the MIT License.</small> </p>