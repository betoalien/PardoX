---
layout: default
title: "FFI Exports Reference"
nav_order: 5
---

# PardoX Core FFI Exports — v0.3.4

Complete reference for all C-ABI functions exported by the PardoX Rust core. Use this to build custom bindings, audit SDK integrations, or test functions directly.

**Total exports:** 186 functions across 5 crates
**Last updated:** 2026-04-01

---

## Table of Contents

1. [pardox_cpu — Main CPU Engine (161 functions)](#pardox_cpu)
2. [pardox_gpu — GPU Acceleration (20 functions)](#pardox_gpu)
3. [pardox_napi — Node.js Bindings (60+ functions)](#pardox_napi)
4. [pardox_wasm — WebAssembly (stub)](#pardox_wasm)
5. [pardox_server — Server Component (stub)](#pardox_server)
6. [Architectural Patterns](#architectural-patterns)

---

## pardox_cpu

**Path:** `Pardox-Core-v0.3.4/pardox_cpu/`
**Crate type:** `cdylib`
**Output:** `pardox-cpu-Linux-x64.so` / `pardox-cpu-Darwin-x64.dylib` / `pardox-cpu-Win-x64.dll`

### Engine Init (1)

| Function | Signature | Description |
|----------|-----------|-------------|
| `pardox_init_engine` | `() -> void` | Initialize engine, MiMalloc allocator, attempt GPU plugin load |

---

### api.rs — Core Lifecycle (6)

| Function | Parameters | Return | Description |
|----------|-----------|--------|-------------|
| `pardox_reset` | `()` | `void` | Clear global HyperBlockManager state |
| `pardox_create_block` | `(col_names_json: *const c_char, col_types_json: *const c_char)` | `*mut HyperBlock` | Allocate a new HyperBlock |
| `pardox_append_block` | `(block_ptr: *mut HyperBlock)` | `i64` | Append HyperBlock to global manager |
| `pardox_free_manager` | `(mgr_ptr: *mut HyperBlockManager)` | `void` | **UNSAFE** — release Rust-allocated manager |
| `pardox_hash_join` | `(left_mgr: *mut HyperBlockManager, right_mgr: *mut HyperBlockManager, left_key: *const c_char, right_key: *const c_char)` | `*mut HyperBlockManager` | Hash join two managers on key columns |
| `pardox_get_system_report` | `()` | `*const c_char` | JSON system info (CPU, memory, version) |

---

### api_core.rs — Mutation & Transformation (3)

| Function | Parameters | Return | Description |
|----------|-----------|--------|-------------|
| `pardox_add_column` | `(target_mgr: *mut HyperBlockManager, source_mgr: *mut HyperBlockManager, name: *const c_char)` | `i64` | Add column from source manager into target |
| `pardox_fill_na` | `(mgr: *mut HyperBlockManager, col_name: *const c_char, value: *const c_char)` | `i64` | Fill null values with given value |
| `pardox_round` | `(mgr: *mut HyperBlockManager, col_name: *const c_char, decimals: i32)` | `i64` | Round float column to N decimal places |

---

### api_reader.rs — Data Ingestion (13)

| Function | Parameters | Return | Description |
|----------|-----------|--------|-------------|
| `pardox_load_manager_csv` | `(path: *const c_char, schema_json: *const c_char, config_json: *const c_char)` | `*mut HyperBlockManager` | Load CSV file (3 args — all required) |
| `pardox_scan_csv` | `(path: *const c_char, schema_json: *const c_char, config_json: *const c_char)` | `i64` | Scan CSV into global manager |
| `pardox_read_json_bytes` | `(bytes_ptr: *const u8, len: usize)` | `*mut HyperBlockManager` | Ingest JSON from raw bytes |
| `pardox_load_manager_prdx` | `(path: *const c_char, limit: i64)` | `*mut HyperBlockManager` | Load .prdx binary file with row limit |
| `pardox_load_manager_parquet` | `(path: *const c_char)` | `*mut HyperBlockManager` | Load Parquet file |
| `pardox_read_head_json` | `(path: *const c_char, limit: i64)` | `*const c_char` | Read first N rows of .prdx as JSON string |
| `pardox_column_sum` | `(path: *const c_char, col: *const c_char)` | `f64` | Sum a column directly from .prdx file |
| `pardox_query_sql` | `(path: *const c_char, query: *const c_char)` | `*const c_char` | Run SQL query on .prdx file, return JSON |
| `pardox_prdx_min` | `(path: *const c_char, col: *const c_char)` | `f64` | Min value directly from .prdx file |
| `pardox_prdx_max` | `(path: *const c_char, col: *const c_char)` | `f64` | Max value directly from .prdx file |
| `pardox_prdx_mean` | `(path: *const c_char, col: *const c_char)` | `f64` | Mean value directly from .prdx file |
| `pardox_prdx_count` | `(path: *const c_char)` | `f64` | Row count directly from .prdx file |
| `pardox_groupby_agg_prdx` | `(path: *const c_char, group_cols_json: *const c_char, agg_json: *const c_char)` | `*mut HyperBlockManager` | GroupBy aggregation directly from .prdx file |

---

### api_writers.rs — Export & Persistence (21)

| Function | Parameters | Return | Description |
|----------|-----------|--------|-------------|
| `pardox_to_csv` | `(mgr: *mut HyperBlockManager, path: *const c_char)` | `i64` | Write manager to CSV file |
| `pardox_to_prdx` | `(mgr: *mut HyperBlockManager, path: *const c_char)` | `i64` | Write manager to .prdx binary format |
| `pardox_write_parquet` | `(path: *const c_char)` | `i64` | Write global manager to Parquet |
| `pardox_write_sharded_parquet` | `(mgr: *mut HyperBlockManager, directory: *const c_char, shards: i32)` | `i64` | Shard-write to Parquet files |
| `pardox_hyper_copy_v3` | `(src_pattern: *const c_char, out_path: *const c_char, schema_json: *const c_char, config_json: *const c_char)` | `i64` | Convert CSV glob pattern → .prdx (bulk ingest) |
| `pardox_version` | `()` | `*const c_char` | Return library version string |
| `pardox_read_schema` | `(path: *const c_char)` | `*const c_char` | Read schema from .prdx file as JSON |
| `pardox_read_head` | `(path: *const c_char, rows: i64)` | `*const c_char` | Read first N rows as JSON |
| `pardox_legacy_ping` | `(system_name: *const c_char)` | `i64` | Backward compat ping |
| `pardox_import_mainframe_dat` | `(path: *const c_char, schema_json: *const c_char)` | `*mut HyperBlockManager` | Import mainframe fixed-width .dat files |
| `pardox_free_string` | `(ptr: *mut c_char)` | `void` | Free Rust-allocated string pointer |
| `pardox_write_sql` | `(mgr: *mut HyperBlockManager, conn_str: *const c_char, table_name: *const c_char, mode: *const c_char, conflict_cols_json: *const c_char)` | `i64` | Write HyperBlockManager to PostgreSQL (COPY for >10k rows, INSERT otherwise) |
| `pardox_write_sql_prdx` | `(prdx_path: *const c_char, conn_str: *const c_char, table_name: *const c_char, mode: *const c_char, conflict_cols_json: *const c_char, batch_rows: i64)` | `i64` | **Stream .prdx file directly to PostgreSQL via COPY FROM STDIN — O(block) RAM.** Added v0.3.2. Error codes: -1=invalid path, -2=invalid conn, -3=invalid table, -4=invalid mode, -5=invalid JSON, -10=file not found, -20=empty conn, -100=engine error |
| `pardox_write_mysql` | `(mgr: *mut HyperBlockManager, conn_str: *const c_char, table_name: *const c_char)` | `i64` | Write to MySQL |
| `pardox_write_sqlserver` | `(mgr: *mut HyperBlockManager, conn_str: *const c_char, table_name: *const c_char)` | `i64` | Write to SQL Server |
| `pardox_write_mongodb` | `(mgr: *mut HyperBlockManager, conn_str: *const c_char, db_collection: *const c_char)` | `i64` | Write to MongoDB |
| `pardox_sql_query` | `(mgr: *mut HyperBlockManager, sql: *const c_char)` | `*mut HyperBlockManager` | Run SQL on in-memory manager |
| `pardox_execute_sql` | `(conn_str: *const c_char, query: *const c_char)` | `*mut HyperBlockManager` | Execute SQL on PostgreSQL, return results |
| `pardox_execute_mysql` | `(conn_str: *const c_char, query: *const c_char)` | `*mut HyperBlockManager` | Execute SQL on MySQL, return results |
| `pardox_execute_sqlserver` | `(conn_str: *const c_char, query: *const c_char)` | `*mut HyperBlockManager` | Execute SQL on SQL Server, return results |
| `pardox_execute_mongodb` | `(conn_str: *const c_char, db_collection: *const c_char, query_json: *const c_char)` | `*mut HyperBlockManager` | Execute query on MongoDB |

---

### api_ext.rs — Advanced Operations (118+)

#### Arrow & Memory

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_ingest_arrow_stream` | `(array_ptr: *const u8, schema_ptr: *const u8)` | `*mut HyperBlockManager` |
| `pardox_ingest_buffer` | `(buffer_ptr: *const u8, buffer_len: usize, layout_json: *const c_char, schema_json: *const c_char)` | `*mut HyperBlockManager` |

#### Inspection & Serialization

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_manager_to_json` | `(mgr: *mut HyperBlockManager, limit: i64)` | `*const c_char` |
| `pardox_manager_to_ascii` | `(mgr: *mut HyperBlockManager, limit: i64)` | `*const c_char` |
| `pardox_manager_to_json_range` | `(mgr: *mut HyperBlockManager, start: i64, limit: i64)` | `*const c_char` |
| `pardox_get_row_count` | `(mgr: *mut HyperBlockManager)` | `i64` |
| `pardox_get_schema_json` | `(mgr: *mut HyperBlockManager)` | `*const c_char` |

#### SQL Database Readers

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_scan_sql` | `(conn_str: *const c_char, query: *const c_char)` | `*mut HyperBlockManager` |
| `pardox_read_mysql` | `(conn_str: *const c_char, query: *const c_char)` | `*mut HyperBlockManager` |
| `pardox_read_sqlserver` | `(conn_str: *const c_char, query: *const c_char)` | `*mut HyperBlockManager` |
| `pardox_read_mongodb` | `(conn_str: *const c_char, db_dot_collection: *const c_char)` | `*mut HyperBlockManager` |

#### JSON Export

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_to_json_records` | `(mgr: *mut HyperBlockManager)` | `*const c_char` |
| `pardox_to_json_arrays` | `(mgr: *mut HyperBlockManager)` | `*const c_char` |

#### EDA

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_value_counts` | `(mgr: *mut HyperBlockManager, col_name: *const c_char)` | `*const c_char` |
| `pardox_unique` | `(mgr: *mut HyperBlockManager, col_name: *const c_char)` | `*const c_char` |

#### Slicing

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_slice_manager` | `(mgr: *mut HyperBlockManager, start: i64, len: i64)` | `*mut HyperBlockManager` |
| `pardox_tail_manager` | `(mgr: *mut HyperBlockManager, n: i64)` | `*mut HyperBlockManager` |

#### Type Casting

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_cast_column` | `(mgr: *mut HyperBlockManager, col_name: *const c_char, target_type_str: *const c_char)` | `i32` |

#### Series Arithmetic

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_series_add` | `(left_mgr, left_col, right_mgr, right_col)` | `*mut HyperBlockManager` |
| `pardox_series_sub` | same pattern | `*mut HyperBlockManager` |
| `pardox_series_mul` | same pattern | `*mut HyperBlockManager` |
| `pardox_series_div` | same pattern | `*mut HyperBlockManager` |
| `pardox_series_mod` | same pattern | `*mut HyperBlockManager` |

#### Filtering & Comparison

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_filter_compare` | `(left_mgr, left_col, right_mgr, right_col, op_code: i32)` | `*mut HyperBlockManager` |
| `pardox_filter_compare_scalar` | `(mgr, col_name, val_f64: f64, val_i64: i64, is_float: i32, op_code: i32)` | `*mut HyperBlockManager` |
| `pardox_apply_filter` | `(data_mgr, mask_mgr, mask_col)` | `*mut HyperBlockManager` |

#### Aggregations

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_agg_sum` | `(mgr, col_name)` | `f64` |
| `pardox_agg_mean` | `(mgr, col_name)` | `f64` |
| `pardox_agg_min` | `(mgr, col_name)` | `f64` |
| `pardox_agg_max` | `(mgr, col_name)` | `f64` |
| `pardox_agg_count` | `(mgr, col_name)` | `f64` |
| `pardox_agg_std` | `(mgr, col_name)` | `f64` |

#### Math

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_math_add` | `(mgr, col_a, col_b)` | `*mut HyperBlockManager` |
| `pardox_math_sub` | `(mgr, col_a, col_b)` | `*mut HyperBlockManager` |
| `pardox_math_stddev` | `(mgr, col_name)` | `f64` |
| `pardox_math_minmax` | `(mgr, col_name)` | `*mut HyperBlockManager` |

#### Sorting

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_sort_values` | `(mgr, col_name, descending: i32)` | `*mut HyperBlockManager` |
| `pardox_gpu_sort` | `(mgr, col_name)` | `*mut HyperBlockManager` |

#### Buffer Access (NumPy / ML integration)

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_get_f64_buffer` | `(mgr, col_name, out_len: *mut usize)` | `*const f64` |

#### GroupBy

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_groupby_agg` | `(mgr, group_cols_json, agg_json)` | `*mut HyperBlockManager` |

---

### Gap-specific Functions

#### Gap 2 — String & Date

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_str_upper` | `(mgr, col_name)` | `*mut HyperBlockManager` |
| `pardox_str_lower` | `(mgr, col_name)` | `*mut HyperBlockManager` |
| `pardox_str_contains` | `(mgr, col_name, pattern)` | `*mut HyperBlockManager` |
| `pardox_str_replace` | `(mgr, col_name, old_str, new_str)` | `*mut HyperBlockManager` |
| `pardox_str_trim` | `(mgr, col_name)` | `*mut HyperBlockManager` |
| `pardox_str_len` | `(mgr, col_name)` | `*mut HyperBlockManager` |
| `pardox_date_extract` | `(mgr, col_name, part: *const c_char)` | `*mut HyperBlockManager` |
| `pardox_date_format` | `(mgr, col_name, format)` | `*mut HyperBlockManager` |
| `pardox_date_diff` | `(mgr, col1, col2, unit)` | `*mut HyperBlockManager` |
| `pardox_date_add` | `(mgr, col_name, days: i64)` | `*mut HyperBlockManager` |

#### Gap 3 — Decimal Type

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_decimal_from_float` | `(mgr, col_name, scale: i32)` | `*mut HyperBlockManager` |
| `pardox_decimal_sum` | `(mgr, col_name)` | `f64` |
| `pardox_decimal_get_scale` | `(mgr, col_name)` | `i32` |
| `pardox_decimal_add` | `(mgr, col_name, val: f64)` | `*mut HyperBlockManager` |
| `pardox_decimal_sub` | `(mgr, col_name, val: f64)` | `*mut HyperBlockManager` |
| `pardox_decimal_mul_float` | `(mgr, col_name, factor: f64)` | `*mut HyperBlockManager` |
| `pardox_decimal_round` | `(mgr, col_name, scale: i32)` | `*mut HyperBlockManager` |
| `pardox_decimal_to_float` | `(mgr, col_name)` | `*mut HyperBlockManager` |

#### Gap 4 — Window Functions

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_row_number` | `(mgr, order_col)` | `*mut HyperBlockManager` |
| `pardox_rank` | `(mgr, order_col)` | `*mut HyperBlockManager` |
| `pardox_lag` | `(mgr, col_name, offset: i64)` | `*mut HyperBlockManager` |
| `pardox_lead` | `(mgr, col_name, offset: i64)` | `*mut HyperBlockManager` |
| `pardox_rolling_mean` | `(mgr, col_name, window_size: i64)` | `*mut HyperBlockManager` |

#### Gap 5 — Lazy Pipeline

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_lazy_scan_csv` | `(path, schema_json, config_json)` | `*mut LazyFrame` |
| `pardox_lazy_scan_prdx` | `(path)` | `*mut LazyFrame` |
| `pardox_lazy_select` | `(lazy_ptr, cols_json)` | `*mut LazyFrame` |
| `pardox_lazy_filter` | `(lazy_ptr, col_name, op, value: f64)` | `*mut LazyFrame` |
| `pardox_lazy_limit` | `(lazy_ptr, n: i64)` | `*mut LazyFrame` |
| `pardox_lazy_collect` | `(lazy_ptr)` | `*mut HyperBlockManager` |
| `pardox_lazy_describe` | `(lazy_ptr)` | `*const c_char` |
| `pardox_lazy_optimize` | `(lazy_ptr)` | `*mut LazyFrame` |
| `pardox_lazy_stats` | `(lazy_ptr)` | `*const c_char` |
| `pardox_lazy_free` | `(lazy_ptr)` | `void` |

#### Gap 8 — Pivot & Melt

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_pivot_table` | `(mgr, index_col, columns_col, values_col, agg_func)` | `*mut HyperBlockManager` |
| `pardox_melt` | `(mgr, id_vars_json, value_vars_json)` | `*mut HyperBlockManager` |

#### Gap 9 — Time Series Fill

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_ffill` | `(mgr, col_name)` | `*mut HyperBlockManager` |
| `pardox_bfill` | `(mgr, col_name)` | `*mut HyperBlockManager` |
| `pardox_interpolate` | `(mgr, col_name, method)` | `*mut HyperBlockManager` |

#### Gap 10 — Nested Data

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_json_extract` | `(mgr, col_name, json_path)` | `*mut HyperBlockManager` |
| `pardox_explode` | `(mgr, col_name)` | `*mut HyperBlockManager` |
| `pardox_unnest` | `(mgr, col_name)` | `*mut HyperBlockManager` |

#### Gap 11 — Spill to Disk

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_spill_to_disk` | `(mgr, path)` | `i64` |
| `pardox_spill_from_disk` | `(path)` | `*mut HyperBlockManager` |
| `pardox_memory_usage` | `(mgr)` | `i64` |

#### Gap 14 — External Sort / Streaming GroupBy

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_external_sort` | `(mgr, col_name)` | `*mut HyperBlockManager` |
| `pardox_chunked_groupby` | `(mgr, group_cols_json, chunk_size: i64)` | `*mut HyperBlockManager` |

#### Gap 15 — Cloud Storage

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_cloud_read_csv` | `(url, config_json)` | `*mut HyperBlockManager` |
| `pardox_cloud_read_prdx` | `(url)` | `*mut HyperBlockManager` |
| `pardox_cloud_write_prdx` | `(mgr, url, config_json)` | `i64` |

#### Gap 16 — Live Query

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_live_query_start` | `(mgr, poll_interval_ms: i64)` | `*mut LiveQuery` |
| `pardox_live_query_version` | `(lq_ptr)` | `i64` |
| `pardox_live_query_take` | `(lq_ptr, n_rows: i64)` | `*mut HyperBlockManager` |
| `pardox_live_query_free` | `(lq_ptr)` | `void` |

#### Gap 18 — Encryption

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_write_prdx_encrypted` | `(mgr, path, password)` | `i64` |
| `pardox_read_prdx_encrypted` | `(path, password)` | `*mut HyperBlockManager` |

#### Gap 19 — Data Contracts

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_validate_contract` | `(mgr, schema_json)` | `i64` |
| `pardox_contract_violation_count` | `()` | `i64` |

#### Gap 20 — Time Travel

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_version_write` | `(mgr, path, version_id)` | `i64` |
| `pardox_version_read` | `(path, version_id)` | `*mut HyperBlockManager` |
| `pardox_version_list` | `(path)` | `*const c_char` |
| `pardox_version_delete` | `(path, version_id)` | `i64` |

#### Gap 21 — Arrow Flight

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_flight_start` | `(port: i32)` | `i64` |
| `pardox_flight_register` | `(path, mgr)` | `i64` |
| `pardox_flight_stop` | `()` | `i32` |
| `pardox_flight_read` | `(table_ref)` | `*mut HyperBlockManager` |

#### Gap 22 — Distributed Cluster

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_cluster_worker_start` | `(listen_addr, port: i32)` | `i64` |
| `pardox_cluster_worker_stop` | `()` | `i32` |
| `pardox_cluster_worker_register` | `(name, listen_addr)` | `i64` |
| `pardox_cluster_connect` | `(worker_addresses_json)` | `i64` |
| `pardox_cluster_scatter` | `(mgr, partitions: i32)` | `*const c_char` |
| `pardox_cluster_scatter_resilient` | `(mgr, partitions: i32, replication_factor: i32)` | `*const c_char` |
| `pardox_cluster_sql` | `(sql_query)` | `*mut HyperBlockManager` |
| `pardox_cluster_sql_resilient` | `(sql_query)` | `*mut HyperBlockManager` |
| `pardox_cluster_ping` | `(worker_name)` | `i64` |
| `pardox_cluster_ping_all` | `()` | `i64` |
| `pardox_cluster_checkpoint` | `(path)` | `i64` |
| `pardox_cluster_free` | `()` | `void` |

#### Gap 23 — SQL Server Fix

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_sqlserver_config_ok` | `()` | `i32` |

#### Gap 28 — Linear Algebra

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_matmul` | `(left_mgr, left_col, right_mgr, right_col)` | `*mut HyperBlockManager` |
| `pardox_l2_normalize` | `(mgr, col_name)` | `*mut HyperBlockManager` |
| `pardox_l1_normalize` | `(mgr, col_name)` | `*mut HyperBlockManager` |
| `pardox_cosine_sim` | `(left_mgr, left_col, right_mgr, right_col)` | `f64` |
| `pardox_pca` | `(mgr, n_components: i32)` | `*mut HyperBlockManager` |

#### Gap 29 — REST Connector

| Function | Parameters | Return |
|----------|-----------|--------|
| `pardox_read_rest` | `(url, method, headers_json)` | `*mut HyperBlockManager` |

#### Gap 30 — SQL Cursor API (added v0.3.3)

Server-side PostgreSQL streaming cursor. Opens a `DECLARE ... NO SCROLL CURSOR` on the server and fetches rows in chunks. Memory usage is O(chunk_size) regardless of query result size. All 5 symbols verified in `libpardox.so`.

| Function | Parameters | Return | Description |
|----------|-----------|--------|-------------|
| `pardox_scan_sql_cursor_open` | `(conn_str: *const c_char, query: *const c_char, chunk_size: usize)` | `*mut SqlCursor` | Open server-side cursor. Returns opaque pointer. `BEGIN` + `DECLARE` issued on the connection |
| `pardox_scan_sql_cursor_fetch` | `(cursor: *mut SqlCursor)` | `*mut HyperBlockManager` | Fetch next chunk. Returns `null` when exhausted |
| `pardox_scan_sql_cursor_offset` | `(cursor: *mut SqlCursor)` | `usize` | Total rows fetched so far |
| `pardox_scan_sql_cursor_close` | `(cursor: *mut SqlCursor)` | `void` | **MUST CALL** — issues `CLOSE cursor` + `COMMIT`, drops `Box<SqlCursor>` |
| `pardox_scan_sql_to_parquet` | `(conn_str: *const c_char, query: *const c_char, output_pattern: *const c_char, chunk_size: i64)` | `i64` | Stream SQL → `.prdx` files using `{i}` pattern. Returns total rows written |

**Python usage:**
```python
for df in px.query_to_results(conn, query, batch_size=50_000):
    records = df.to_dict()

total = px.sql_to_parquet(conn, query, "/tmp/chunk_{i}.prdx", chunk_size=100_000)
```

---

## pardox_gpu

**Path:** `Pardox-Core-v0.3.4/pardox_gpu/`
**Crate type:** `cdylib`
**Output:** `pardox-gpu-Linux-x64.so`
**Backend:** `wgpu` (WebGPU — cross-platform: Vulkan, Metal, DX12, OpenGL ES)

### Init

| Function | Parameters | Return | Description |
|----------|-----------|--------|-------------|
| `gpu_init` | `()` | `i64` | Initialize wgpu device; 0 = success, -1 = no adapter |

### Binary Arithmetic (element-wise, f64)

| Function | Parameters | Return |
|----------|-----------|--------|
| `gpu_add_f64` | `(ptr_a: *const f64, ptr_b: *const f64, len: usize, ptr_out: *mut f64)` | `i64` |
| `gpu_sub_f64` | same pattern | `i64` |
| `gpu_mul_f64` | same pattern | `i64` |
| `gpu_div_f64` | same pattern | `i64` |
| `gpu_mod_f64` | same pattern | `i64` |
| `gpu_pow_f64` | same pattern | `i64` |
| `gpu_max_elementwise_f64` | same pattern | `i64` |
| `gpu_min_elementwise_f64` | same pattern | `i64` |

### Reductions

| Function | Parameters | Return |
|----------|-----------|--------|
| `gpu_sum_f64` | `(ptr_in: *const f64, len: usize, ptr_out: *mut f64)` | `i64` |
| `gpu_min_f64` | same pattern | `i64` |
| `gpu_max_f64` | same pattern | `i64` |
| `gpu_reduce_sum_f64` | same pattern (multi-pass) | `i64` |
| `gpu_reduce_min_f64` | same pattern | `i64` |
| `gpu_reduce_max_f64` | same pattern | `i64` |

### Sorting

| Function | Parameters | Return | Description |
|----------|-----------|--------|-------------|
| `gpu_sort_f64` | `(ptr_in: *const f64, len: usize, ptr_out: *mut f64, descending: i32)` | `i64` | Bitonic sort on GPU |

### Unary Math

| Function | Parameters | Return |
|----------|-----------|--------|
| `gpu_sqrt_f64` | `(ptr_in: *const f64, len: usize, ptr_out: *mut f64)` | `i64` |
| `gpu_exp_f64` | same pattern | `i64` |
| `gpu_log_f64` | same pattern | `i64` |
| `gpu_abs_f64` | same pattern | `i64` |

---

## pardox_napi

**Path:** `Pardox-Core-v0.3.4/pardox_napi/`
**Crate type:** `cdylib`
**Output:** `pardox.node` (Node.js native addon)
**Binding framework:** `napi-rs`

### DataFrame class (30+ instance methods)

| Method | Description |
|--------|-------------|
| `fromCsv(path, schema, config)` | Load CSV |
| `fromPrdx(path, limit)` | Load .prdx |
| `toJson(limit)` | Serialize to JSON records |
| `toAscii(limit)` | Pretty-print table |
| `groupBy(groupCols, aggJson)` | GroupBy aggregation |
| `sortValues(col, desc)` | Sort |
| `filter(col, op, value)` | Scalar filter |
| `select(cols)` | Column selection |
| `head(n)` / `tail(n)` | Row slicing |
| `getSchema()` | Schema as JSON |
| `rowCount()` | Row count |
| `sum/mean/min/max/std(col)` | Aggregations |
| `toCsv(path)` / `toPrdx(path)` | Export |
| `strUpper/Lower/Contains/Replace/Trim(col, ...)` | String ops |
| `dateExtract/Format/Diff/Add(col, ...)` | Date ops |
| `lag/lead/rank/rowNumber/rollingMean(col, ...)` | Window ops |
| `pivot/melt(...)` | Reshape |
| `ffill/bfill/interpolate(col, ...)` | Time series fill |
| `jsonExtract/explode/unnest(col, ...)` | Nested data |
| `spillToDisk/spillFromDisk(path)` | Spill |
| `encryptedWrite/encryptedRead(path, key)` | Encryption |
| `validateContract(schemaJson)` | Contracts |
| `versionWrite/Read/List/Delete(...)` | Time travel |
| `l2Normalize/l1Normalize/cosineSim/pca/matmul(...)` | LinAlg |
| `free()` | Release memory |

All pardox_cpu functions are also exposed as module-level exports with camelCase naming (60+ total).

---

## pardox_wasm

**Path:** `Pardox-Core-v0.3.4/pardox_wasm/`
**Target:** `wasm32-unknown-unknown`
**Binding framework:** `wasm-bindgen`
**Status:** Stub / development placeholder

Exports a minimal `PardoXWasm` class with placeholder methods. Full WASM port planned.

---

## pardox_server

**Path:** `Pardox-Core-v0.3.4/pardox_server/`
**Status:** Empty scaffold — no exported functions

The PostgreSQL wire-protocol server is implemented in Python (`pardox_validation/py_validation/pardox/server/`), not in this Rust crate.

---

## Architectural Patterns

### Memory Ownership

- All `*mut HyperBlockManager` pointers are **Rust-owned** — caller must call `pardox_free_manager(ptr)` when done
- String returns (`*const c_char`) are also Rust-owned — call `pardox_free_string(ptr)` to release
- `LazyFrame*` pointers are consumed by `pardox_lazy_collect()` — do not free separately after collect

### Error Signaling

- `i64` returns: `>= 0` = success, `< 0` = error code
- `*mut T` returns: `NULL` = error
- `f64` returns: `NaN` or negative sentinel for aggregation errors

### FFI Calling Conventions

- All functions: `extern "C"` with `#[no_mangle]`
- All strings: null-terminated UTF-8 C strings (`*const c_char`)
- Boolean flags: `i32` (0 = false, 1 = true)

### Key FFI Gotchas

1. `pardox_load_manager_csv` requires exactly 3 args: `(path, schema_json, config_json)` — both JSON args can be `"{}"` but must not be NULL
2. `pardox_get_f64_buffer` takes `out_len: *mut usize` — caller provides pointer to receive length
3. `pardox_lazy_filter` arg order: `(lazy_ptr, col_name, op_str, value: f64)` — op is lowercase (`"gt"`, `"lt"`, `"eq"`, `"neq"`, `"gte"`, `"lte"`)
4. `pardox_lazy_select` expects JSON array string: `'["col1","col2"]'`
5. Decimal type code in schema JSON: `0x80 | scale` (e.g., scale=2 → type code `130`)
6. `pardox_hyper_copy_v3` is for CSV→PRDX bulk conversion, NOT PostgreSQL COPY protocol

### Platform Binaries

| Platform | CPU | GPU | NAPI |
|----------|-----|-----|------|
| Linux x64 | `pardox-cpu-Linux-x64.so` | `pardox-gpu-Linux-x64.so` | `pardox-Linux-x64.node` |
| macOS Intel | `pardox-cpu-Darwin-x64.dylib` | `pardox-gpu-Darwin-x64.dylib` | `pardox-Darwin-x64.node` |
| macOS ARM | `pardox-cpu-Darwin-arm64.dylib` | `pardox-gpu-Darwin-arm64.dylib` | `pardox-Darwin-arm64.node` |
| Windows | `pardox-cpu-Win-x64.dll` | `pardox-gpu-Win-x64.dll` | `pardox-Win-x64.node` |
