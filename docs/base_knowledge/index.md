---
layout: default
title: "Base Knowledge"
nav_order: 6
has_children: true
---

# Base Knowledge

Validation scripts for PardoX v0.3.2 across all supported SDKs. Each file is a standalone, runnable example that verifies one or more features of the SDK.

---

## SDKs

| Folder | SDK | Install | Files |
|--------|-----|---------|-------|
| [`python_code/`](python_code/) | Python 3.8+ | `pip install pardox` | 28 |
| [`js_code/`](js_code/) | Node.js 18+ | `npm i @pardox/pardox` | 18 |
| [`php_code/`](php_code/) | PHP 8.1+ | `composer require betoalien/pardox-php` | 28 |

---

## Gaps covered

| File pattern | Feature |
|--------------|---------|
| `validate_gap1_sdk.*` | GroupBy Aggregation |
| `validate_gap2_sdk.*` | String & Date Operations |
| `validate_gap3_sdk.*` | Decimal Type |
| `validate_gap4_sdk.*` | Window Functions |
| `validate_gap5_sdk.*` | Lazy Pipeline |
| `validate_gap6_sdk.*` | PostgreSQL Wire Server |
| `validate_gap7_sdk.*` | GPU Sort |
| `validate_gap8_sdk.*` | Pivot & Melt |
| `validate_gap9_sdk.*` | Time Series Fill |
| `validate_gap10_sdk.*` | Nested / JSON Data |
| `validate_gap11_sdk.*` | Out-of-Core / Spill to Disk |
| `validate_gap12_sdk.*` | PRDX Loader |
| `validate_gap13_sdk.*` | Streaming GroupBy on .prdx |
| `validate_gap14_sdk.*` | SQL over DataFrames |
| `validate_gap15_sdk.*` | Cloud Storage (S3 / GCS / Azure) |
| `validate_gap16_sdk.*` | Live Query |
| `validate_gap17_sdk.*` | WebAssembly |
| `validate_gap18_sdk.*` | Encryption |
| `validate_gap19_sdk.*` | Data Contracts *(Python, PHP only)* |
| `validate_gap20_sdk.*` | Time Travel *(Python, PHP only)* |
| `validate_gap21_sdk.*` | Arrow Flight *(Python, PHP only)* |
| `validate_gap22_sdk.*` | Distributed Cluster *(Python, PHP only)* |
| `validate_gap23_sdk.*` | SQL Server *(Python, PHP only)* |
| `validate_gap24_sdk.*` | Hash JOIN *(Python, PHP only)* |
| `validate_gap26_sdk.*` | Fault Tolerance *(Python, PHP only)* |
| `validate_gap27_sdk.*` | Query Planner *(Python, PHP only)* |
| `validate_gap28_sdk.*` | Linear Algebra *(Python, PHP only)* |
| `validate_gap29_sdk.*` | REST Connector *(Python, PHP only)* |

---

## How to run

```bash
# Python
python validate_gap1_sdk.py

# Node.js
node validate_gap1_sdk.js

# PHP
php validate_gap1_sdk.php
```

Each script prints a results summary. All tests should show `ALL TESTS PASSED ✓`.
