---
layout: default
title: "Installation"
parent: "Getting Started"
nav_order: 1
---

# Installation

PardoX ships as a pre-compiled native library. No Rust compiler or C toolchain is required on the end-user machine. The Rust core (`libpardox.so` / `.dylib` / `.dll`) is bundled inside the package for each supported platform.

---

## Python SDK

### via pip

```bash
pip install pardox
```

### System Requirements

| OS | Architecture | Status |
|----|-------------|--------|
| Linux | x86_64 | ✅ Stable |
| Windows | x86_64 | ✅ Stable |
| macOS | ARM64 (M1/M2/M3) | ✅ Stable |
| macOS | x86_64 (Intel) | ✅ Stable |

!!! note "Python version"
    Python **3.8 or higher** is required. No Rust compiler is needed — binaries are pre-packaged for all platforms.

!!! note "No database drivers needed"
    PardoX connects to PostgreSQL, MySQL, SQL Server, and MongoDB entirely through the Rust core. You do **not** need to install `psycopg2`, `pymysql`, `pyodbc`, or `pymongo`.

### Verify Installation

```python
import pardox as px
print(px.__version__)    # e.g. "0.3.2"

df = px.read_csv("my_data.csv")
print(df.shape)
```

---

## Node.js SDK

The Node.js SDK uses [koffi](https://koffi.dev/) for FFI bindings and ships the same pre-compiled `.so` / `.dylib` / `.dll`.

```bash
npm i @pardox/pardox
```

### Usage

```js
const { DataFrame, read_csv } = require('@pardox/pardox');

const df = px.read_csv('my_data.csv');
console.log(df.shape);
```

### Requirements

| Runtime | Minimum version |
|---------|----------------|
| Node.js | 18 LTS or higher |

---

## PHP SDK

The PHP SDK uses the built-in `FFI` extension (PHP 7.4+).

```bash
composer require betoalien/pardox-php
```

### Requirements

| Runtime | Minimum version |
|---------|----------------|
| PHP | 8.1 or higher |
| FFI extension | Must be enabled in `php.ini` |

Enable the FFI extension:

```ini
; php.ini
extension=ffi
ffi.enable=true
```

### Usage

```php
<?php
require_once 'vendor/autoload.php';

use PardoX\DataFrame;

$df = DataFrame::read_csv('my_data.csv');
echo $df->shape[0] . " rows\n";
```

---

## Docker (Databases for Testing)

A `docker-compose.yml` is provided to spin up all supported databases locally:

```bash
docker-compose up -d
```

This starts:

| Container | Database | Host port |
|-----------|----------|-----------|
| `pardox_validation_db` | PostgreSQL 16 | 5434 |
| `pardox_mysql` | MySQL 8.0 | 3306 |
| `pardox_sqlserver` | SQL Server 2022 | 1433 |

!!! tip "MongoDB"
    Add a MongoDB service to the compose file with `mongo:7.0` if needed for your environment.
