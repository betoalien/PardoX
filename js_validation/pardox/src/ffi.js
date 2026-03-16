'use strict';

/**
 * PardoX FFI Singleton (Node.js)
 *
 * Loads the Rust shared library via koffi and registers all exported C-ABI
 * functions. Returns the same koffi library instance on every call (singleton).
 *
 * Mirrors Python's wrapper.py and PHP's Core/FFI.php.
 *
 * String memory management pattern:
 *   Functions that return heap-allocated char* are declared as returning
 *   'void *' (opaque pointer). The caller reads the string with
 *   koffi.decode(ptr, 'string') and then frees with pardox_free_string(ptr).
 *
 * 64-bit integer return values (long long / size_t):
 *   koffi returns these as BigInt. Helper functions convert to Number where safe.
 */

const koffi = require('koffi');
const { getLibraryPath } = require('./lib');

/** @type {Object|null} */
let _lib = null;

/**
 * Returns the koffi library instance, loading it on first call.
 * @returns {Object} koffi lib with all function bindings.
 */
function getLib() {
    if (_lib !== null) return _lib;

    const libPath = getLibraryPath();

    let raw;
    try {
        raw = koffi.load(libPath);
    } catch (err) {
        throw new Error(`Fatal Error loading PardoX: ${err.message}`);
    }

    // -------------------------------------------------------------------------
    // Engine Initialization
    // -------------------------------------------------------------------------
    const pardox_init_engine = raw.func('void pardox_init_engine()');

    // -------------------------------------------------------------------------
    // Memory Management
    // -------------------------------------------------------------------------
    const pardox_free_manager = raw.func('void pardox_free_manager(void * mgr)');
    const pardox_free_string  = raw.func('void pardox_free_string(void * ptr)');

    // -------------------------------------------------------------------------
    // Ingestion
    // -------------------------------------------------------------------------
    const pardox_read_json_bytes    = raw.func('void * pardox_read_json_bytes(const char * bytes, size_t len)');
    const pardox_load_manager_csv   = raw.func('void * pardox_load_manager_csv(const char * path, const char * schema, const char * config)');

    // -------------------------------------------------------------------------
    // Native SQL Read
    // -------------------------------------------------------------------------
    const pardox_scan_sql = raw.func('void * pardox_scan_sql(const char * conn_str, const char * query)');

    // -------------------------------------------------------------------------
    // Joins
    // -------------------------------------------------------------------------
    const pardox_hash_join = raw.func('void * pardox_hash_join(void * left, void * right, const char * left_key, const char * right_key)');

    // -------------------------------------------------------------------------
    // Slicing & Navigation
    // -------------------------------------------------------------------------
    const pardox_slice_manager = raw.func('void * pardox_slice_manager(void * mgr, size_t start, size_t len)');
    const pardox_tail_manager  = raw.func('void * pardox_tail_manager(void * mgr, size_t n)');

    // -------------------------------------------------------------------------
    // Inspection
    // Note: functions using set_global_buffer() return thread-local pointers
    //       declared as 'const char *' so koffi auto-decodes without freeing.
    //       Functions using CString::into_raw() are declared as 'void *'
    //       so we can manually decode and free the heap allocation.
    // -------------------------------------------------------------------------
    const pardox_get_row_count       = raw.func('int64_t pardox_get_row_count(void * mgr)');
    // Thread-local buffer (set_global_buffer) — koffi auto-decodes 'const char *'
    const pardox_get_schema_json     = raw.func('const char * pardox_get_schema_json(void * mgr)');
    const pardox_manager_to_json     = raw.func('const char * pardox_manager_to_json(void * mgr, size_t limit)');
    const pardox_manager_to_ascii    = raw.func('const char * pardox_manager_to_ascii(void * mgr, size_t limit)');
    const pardox_manager_to_json_range = raw.func('const char * pardox_manager_to_json_range(void * mgr, size_t start, size_t limit)');

    // -------------------------------------------------------------------------
    // Casting & Column Mutation
    // -------------------------------------------------------------------------
    const pardox_cast_column = raw.func('int pardox_cast_column(void * mgr, const char * col_name, const char * target_type)');
    const pardox_add_column  = raw.func('int64_t pardox_add_column(void * target, void * source, const char * col_name)');

    // -------------------------------------------------------------------------
    // Cleaning
    // -------------------------------------------------------------------------
    const pardox_fill_na = raw.func('int64_t pardox_fill_na(void * mgr, const char * col, double val)');
    const pardox_round   = raw.func('int64_t pardox_round(void * mgr, const char * col, int decimals)');

    // -------------------------------------------------------------------------
    // Writers
    // -------------------------------------------------------------------------
    const pardox_to_csv    = raw.func('int64_t pardox_to_csv(void * mgr, const char * path)');
    const pardox_to_prdx   = raw.func('int64_t pardox_to_prdx(void * mgr, const char * path)');
    const pardox_write_sql = raw.func('int64_t pardox_write_sql(void * mgr, const char * conn_str, const char * table, const char * mode, const char * conflict_cols_json)');
    const pardox_execute_sql = raw.func('int64_t pardox_execute_sql(const char * conn_str, const char * query)');

    // -------------------------------------------------------------------------
    // Gap 15: Cloud Storage (S3/GCS/Azure)
    // -------------------------------------------------------------------------
    const pardox_cloud_read_csv = raw.func('void * pardox_cloud_read_csv(const char * url, const char * schema, const char * config, const char * credentials)');
    const pardox_cloud_read_prdx = raw.func('void * pardox_cloud_read_prdx(const char * url, int64_t limit, const char * credentials)');
    const pardox_cloud_write_prdx = raw.func('int64_t pardox_cloud_write_prdx(void * mgr, const char * url, const char * credentials)');

    // Gap 14: Streaming PRDX to SQL - COMMENTED: Function not available in Core
    // const pardox_write_sql_prdx = raw.func('int64_t pardox_write_sql_prdx(const char * prdx_path, const char * conn_str, const char * table_name, const char * mode, const char * conflict_cols_json, int64_t batch_rows)');

    // -------------------------------------------------------------------------
    // Arithmetic (Series)
    // -------------------------------------------------------------------------
    const pardox_series_add = raw.func('void * pardox_series_add(void * left, const char * left_col, void * right, const char * right_col)');
    const pardox_series_sub = raw.func('void * pardox_series_sub(void * left, const char * left_col, void * right, const char * right_col)');
    const pardox_series_mul = raw.func('void * pardox_series_mul(void * left, const char * left_col, void * right, const char * right_col)');
    const pardox_series_div = raw.func('void * pardox_series_div(void * left, const char * left_col, void * right, const char * right_col)');
    const pardox_series_mod = raw.func('void * pardox_series_mod(void * left, const char * left_col, void * right, const char * right_col)');

    // -------------------------------------------------------------------------
    // Filter Predicates
    // -------------------------------------------------------------------------
    const pardox_filter_compare = raw.func(
        'void * pardox_filter_compare(void * left, const char * left_col, void * right, const char * right_col, int op_code)'
    );
    const pardox_filter_compare_scalar = raw.func(
        'void * pardox_filter_compare_scalar(void * mgr, const char * col, double val_f64, int64_t val_i64, int is_float, int op_code)'
    );
    const pardox_apply_filter = raw.func('void * pardox_apply_filter(void * data, void * mask, const char * mask_col)');

    // -------------------------------------------------------------------------
    // Aggregations
    // -------------------------------------------------------------------------
    const pardox_agg_sum   = raw.func('double pardox_agg_sum(void * mgr, const char * col)');
    const pardox_agg_mean  = raw.func('double pardox_agg_mean(void * mgr, const char * col)');
    const pardox_agg_min   = raw.func('double pardox_agg_min(void * mgr, const char * col)');
    const pardox_agg_max   = raw.func('double pardox_agg_max(void * mgr, const char * col)');
    const pardox_agg_count = raw.func('double pardox_agg_count(void * mgr, const char * col)');
    const pardox_agg_std   = raw.func('double pardox_agg_std(void * mgr, const char * col)');

    // -------------------------------------------------------------------------
    // MySQL
    // -------------------------------------------------------------------------
    const pardox_read_mysql    = raw.func('void * pardox_read_mysql(const char * conn_str, const char * query)');
    const pardox_write_mysql   = raw.func('int64_t pardox_write_mysql(void * mgr, const char * conn_str, const char * table, const char * mode, const char * conflict_cols_json)');
    const pardox_execute_mysql = raw.func('int64_t pardox_execute_mysql(const char * conn_str, const char * query)');

    // -------------------------------------------------------------------------
    // SQL Server
    // -------------------------------------------------------------------------
    const pardox_read_sqlserver    = raw.func('void * pardox_read_sqlserver(const char * conn_str, const char * query)');
    const pardox_write_sqlserver   = raw.func('int64_t pardox_write_sqlserver(void * mgr, const char * conn_str, const char * table, const char * mode, const char * conflict_cols_json)');
    const pardox_execute_sqlserver = raw.func('int64_t pardox_execute_sqlserver(const char * conn_str, const char * query)');

    // -------------------------------------------------------------------------
    // MongoDB
    // -------------------------------------------------------------------------
    const pardox_read_mongodb    = raw.func('void * pardox_read_mongodb(const char * conn_str, const char * db_dot_collection)');
    const pardox_write_mongodb   = raw.func('int64_t pardox_write_mongodb(void * mgr, const char * conn_str, const char * db_dot_collection, const char * mode)');
    const pardox_execute_mongodb = raw.func('int64_t pardox_execute_mongodb(const char * conn_str, const char * database, const char * command_json)');

    // -------------------------------------------------------------------------
    // Native Math Foundation v0.3.1
    // -------------------------------------------------------------------------
    const pardox_math_add    = raw.func('void * pardox_math_add(void * mgr, const char * col_a, const char * col_b)');
    const pardox_math_sub    = raw.func('void * pardox_math_sub(void * mgr, const char * col_a, const char * col_b)');
    const pardox_math_stddev = raw.func('double pardox_math_stddev(void * mgr, const char * col_name)');
    const pardox_math_minmax = raw.func('void * pardox_math_minmax(void * mgr, const char * col_name)');
    const pardox_sort_values = raw.func('void * pardox_sort_values(void * mgr, const char * col_name, int descending)');
    const pardox_gpu_sort    = raw.func('void * pardox_gpu_sort(void * mgr, const char * col_name)');

    // -------------------------------------------------------------------------
    // Gap 1: GroupBy Aggregation
    // -------------------------------------------------------------------------
    const pardox_groupby_agg = raw.func('void * pardox_groupby_agg(void * mgr, const char * group_cols_json, const char * agg_json)');

    // -------------------------------------------------------------------------
    // Gap 2: String Functions
    // -------------------------------------------------------------------------
    const pardox_str_upper     = raw.func('void * pardox_str_upper(void * mgr, const char * col_name)');
    const pardox_str_lower     = raw.func('void * pardox_str_lower(void * mgr, const char * col_name)');
    const pardox_str_contains  = raw.func('void * pardox_str_contains(void * mgr, const char * col_name, const char * pattern)');
    const pardox_str_replace   = raw.func('void * pardox_str_replace(void * mgr, const char * col_name, const char * old_str, const char * new_str)');
    const pardox_str_trim      = raw.func('void * pardox_str_trim(void * mgr, const char * col_name)');
    const pardox_str_len       = raw.func('void * pardox_str_len(void * mgr, const char * col_name)');

    // -------------------------------------------------------------------------
    // Gap 2: Date Functions
    // -------------------------------------------------------------------------
    const pardox_date_extract = raw.func('void * pardox_date_extract(void * mgr, const char * col_name, const char * part)');
    const pardox_date_format = raw.func('void * pardox_date_format(void * mgr, const char * col_name, const char * fmt)');
    const pardox_date_add    = raw.func('void * pardox_date_add(void * mgr, const char * col_name, int64_t days)');
    const pardox_date_diff   = raw.func('void * pardox_date_diff(void * mgr, const char * col_a, const char * col_b, const char * unit)');

    // -------------------------------------------------------------------------
    // Gap 3: Decimal Functions
    // -------------------------------------------------------------------------
    const pardox_decimal_from_float = raw.func('void * pardox_decimal_from_float(void * mgr, const char * col_name, uint8_t scale)');
    const pardox_decimal_to_float   = raw.func('void * pardox_decimal_to_float(void * mgr, const char * col_name)');
    const pardox_decimal_add       = raw.func('void * pardox_decimal_add(void * mgr, const char * col_a, const char * col_b)');
    const pardox_decimal_sub       = raw.func('void * pardox_decimal_sub(void * mgr, const char * col_a, const char * col_b)');
    const pardox_decimal_mul_float = raw.func('void * pardox_decimal_mul_float(void * mgr, const char * col_name, double factor)');
    const pardox_decimal_round     = raw.func('void * pardox_decimal_round(void * mgr, const char * col_name, uint8_t scale)');
    const pardox_decimal_sum       = raw.func('double pardox_decimal_sum(void * mgr, const char * col_name)');
    const pardox_decimal_get_scale = raw.func('int pardox_decimal_get_scale(void * mgr, const char * col_name)');

    // -------------------------------------------------------------------------
    // Gap 4: Window Functions
    // -------------------------------------------------------------------------
    const pardox_row_number   = raw.func('void * pardox_row_number(void * mgr, const char * order_col, int ascending)');
    const pardox_rank         = raw.func('void * pardox_rank(void * mgr, const char * order_col, int ascending)');
    const pardox_lag          = raw.func('void * pardox_lag(void * mgr, const char * col_name, int64_t offset, double fill)');
    const pardox_lead         = raw.func('void * pardox_lead(void * mgr, const char * col_name, int64_t offset, double fill)');
    const pardox_rolling_mean = raw.func('void * pardox_rolling_mean(void * mgr, const char * col_name, int64_t window)');

    // -------------------------------------------------------------------------
    // Gap 5: Lazy Pipeline
    // -------------------------------------------------------------------------
    const pardox_lazy_scan_csv = raw.func('void * pardox_lazy_scan_csv(const char * path, int delimiter, int has_header)');
    const pardox_lazy_select   = raw.func('void * pardox_lazy_select(void * frame, const char * cols_json)');
    const pardox_lazy_filter   = raw.func('void * pardox_lazy_filter(void * frame, const char * col, const char * op, double value)');
    const pardox_lazy_limit    = raw.func('void * pardox_lazy_limit(void * frame, int64_t limit)');
    const pardox_lazy_collect  = raw.func('void * pardox_lazy_collect(void * frame)');
    const pardox_lazy_describe = raw.func('const char * pardox_lazy_describe(void * frame)');
    const pardox_lazy_free     = raw.func('void pardox_lazy_free(void * frame)');

    // -------------------------------------------------------------------------
    // Gap 7: GPU Compute
    // -------------------------------------------------------------------------
    const pardox_gpu_add   = raw.func('void * pardox_gpu_add(void * mgr, const char * col_a, const char * col_b)');
    const pardox_gpu_sub   = raw.func('void * pardox_gpu_sub(void * mgr, const char * col_a, const char * col_b)');
    const pardox_gpu_mul   = raw.func('void * pardox_gpu_mul(void * mgr, const char * col_a, const char * col_b)');
    const pardox_gpu_div   = raw.func('void * pardox_gpu_div(void * mgr, const char * col_a, const char * col_b)');
    const pardox_gpu_sqrt  = raw.func('void * pardox_gpu_sqrt(void * mgr, const char * col)');
    const pardox_gpu_exp   = raw.func('void * pardox_gpu_exp(void * mgr, const char * col)');
    const pardox_gpu_log   = raw.func('void * pardox_gpu_log(void * mgr, const char * col)');
    const pardox_gpu_abs   = raw.func('void * pardox_gpu_abs(void * mgr, const char * col)');

    // -------------------------------------------------------------------------
    // Gap 8: Pivot & Melt
    // -------------------------------------------------------------------------
    const pardox_pivot_table = raw.func('void * pardox_pivot_table(void * mgr, const char * index, const char * columns, const char * values, const char * agg_func)');
    const pardox_melt        = raw.func('void * pardox_melt(void * mgr, const char * id_vars_json, const char * value_vars_json, const char * var_name, const char * value_name)');

    // -------------------------------------------------------------------------
    // Gap 9: Time Series Fill
    // -------------------------------------------------------------------------
    const pardox_ffill       = raw.func('void * pardox_ffill(void * mgr, const char * col)');
    const pardox_bfill       = raw.func('void * pardox_bfill(void * mgr, const char * col)');
    const pardox_interpolate = raw.func('void * pardox_interpolate(void * mgr, const char * col)');

    // -------------------------------------------------------------------------
    // Gap 10: Nested Data
    // -------------------------------------------------------------------------
    const pardox_json_extract = raw.func('void * pardox_json_extract(void * mgr, const char * col, const char * key)');
    const pardox_explode      = raw.func('void * pardox_explode(void * mgr, const char * col)');
    const pardox_unnest       = raw.func('void * pardox_unnest(void * mgr, const char * col)');

    // -------------------------------------------------------------------------
    // Gap 11: Spill to Disk
    // -------------------------------------------------------------------------
    const pardox_spill_to_disk    = raw.func('int64_t pardox_spill_to_disk(void * mgr, const char * path)');
    const pardox_spill_from_disk  = raw.func('void * pardox_spill_from_disk(const char * path)');
    const pardox_chunked_groupby  = raw.func('void * pardox_chunked_groupby(void * mgr, const char * group_col, const char * agg_json, int64_t chunk_size)');
    const pardox_external_sort    = raw.func('void * pardox_external_sort(void * mgr, const char * col, int32_t ascending, int64_t chunk_size)');
    const pardox_memory_usage     = raw.func('int64_t pardox_memory_usage(void * mgr)');

    // -------------------------------------------------------------------------
    // Observer v0.3.1 — Native Universal Export & Vectorized Inspection
    // These functions return heap-allocated strings via CString::into_raw().
    // Declared as 'void *' so koffi returns an opaque pointer; we then read
    // the string with koffi.decode() and immediately free with pardox_free_string.
    // -------------------------------------------------------------------------
    const pardox_to_json_records = raw.func('void * pardox_to_json_records(void * mgr)');
    const pardox_to_json_arrays  = raw.func('void * pardox_to_json_arrays(void * mgr)');
    const pardox_value_counts    = raw.func('void * pardox_value_counts(void * mgr, const char * col_name)');
    const pardox_unique          = raw.func('void * pardox_unique(void * mgr, const char * col_name)');

    // -------------------------------------------------------------------------
    // Native Readers
    // -------------------------------------------------------------------------
    // Thread-local buffer (set_global_buffer) — koffi auto-decodes 'const char *'
    const pardox_read_head_json = raw.func('const char * pardox_read_head_json(const char * path, size_t limit)');
    const pardox_column_sum     = raw.func('double pardox_column_sum(const char * path, const char * col)');
    const pardox_query_sql      = raw.func('const char * pardox_query_sql(const char * path, const char * query)');

    // -------------------------------------------------------------------------
    // Gap 12: PRDX Loader
    // -------------------------------------------------------------------------
    const pardox_load_manager_prdx = raw.func('void * pardox_load_manager_prdx(const char * path, int64_t limit)');

    // Gap 16: Live Query / Auto-Refresh
    // -------------------------------------------------------------------------
    const pardox_live_query_start   = raw.func('void * pardox_live_query_start(const char * prdx_path, const char * sql_query, int poll_ms)');
    const pardox_live_query_version = raw.func('int pardox_live_query_version(void * lq)');
    const pardox_live_query_take    = raw.func('void * pardox_live_query_take(void * lq)');
    const pardox_live_query_free    = raw.func('void pardox_live_query_free(void * lq)');

    // -------------------------------------------------------------------------
    // Gap 13: Streaming GroupBy & Aggregations over .prdx
    // -------------------------------------------------------------------------
    const pardox_prdx_min  = raw.func('double pardox_prdx_min(const char * path, const char * col)');
    const pardox_prdx_max  = raw.func('double pardox_prdx_max(const char * path, const char * col)');
    const pardox_prdx_mean = raw.func('double pardox_prdx_mean(const char * path, const char * col)');
    const pardox_prdx_count = raw.func('int64_t pardox_prdx_count(const char * path)');
    const pardox_groupby_agg_prdx = raw.func('void * pardox_groupby_agg_prdx(const char * path, const char * group_cols_json, const char * agg_json)');

    // -------------------------------------------------------------------------
    // Gap 18: Encryption (PRDX level)
    // -------------------------------------------------------------------------
    const pardox_write_prdx_encrypted = raw.func('int pardox_write_prdx_encrypted(void * mgr, const char * path, const char * password)');
    const pardox_read_prdx_encrypted = raw.func('void * pardox_read_prdx_encrypted(const char * path, const char * password)');

    // -------------------------------------------------------------------------
    // Gap 14: SQL Query over in-memory DataFrame
    // -------------------------------------------------------------------------
    const pardox_sql_query = raw.func('void * pardox_sql_query(void * mgr, const char * sql)');

    // -------------------------------------------------------------------------
    // Parquet Support
    // -------------------------------------------------------------------------
    const pardox_to_parquet           = raw.func('int64_t pardox_to_parquet(void * mgr, const char * path)');
    const pardox_write_sharded_parquet = raw.func('int64_t pardox_write_sharded_parquet(void * mgr, const char * directory, int64_t max_rows_per_shard)');
    const pardox_load_manager_parquet  = raw.func('void * pardox_load_manager_parquet(const char * path)');

    // -------------------------------------------------------------------------
    // PRDX File Metadata
    // -------------------------------------------------------------------------
    const pardox_read_schema       = raw.func('const char * pardox_read_schema(const char * path)');
    const pardox_inspect_file_lazy = raw.func('const char * pardox_inspect_file_lazy(const char * path, const char * options_json)');

    // -------------------------------------------------------------------------
    // Raw Buffer Ingestion
    // -------------------------------------------------------------------------
    const pardox_ingest_buffer = raw.func('void * pardox_ingest_buffer(void * buffer_ptr, size_t buffer_len, const char * layout_json, const char * schema_json)');

    // -------------------------------------------------------------------------
    // Bulk CSV to PRDX
    // -------------------------------------------------------------------------
    const pardox_hyper_copy_v3 = raw.func('int64_t pardox_hyper_copy_v3(const char * src_pattern, const char * out_path, const char * schema_json, const char * config_json)');

    // -------------------------------------------------------------------------
    // Mainframe Import
    // -------------------------------------------------------------------------
    const pardox_import_mainframe_dat = raw.func('void * pardox_import_mainframe_dat(const char * path, const char * schema_json)');

    // -------------------------------------------------------------------------
    // Engine Utilities
    // -------------------------------------------------------------------------
    const pardox_reset               = raw.func('void pardox_reset()');
    const pardox_legacy_ping         = raw.func('int64_t pardox_legacy_ping(const char * system_name)');
    const pardox_get_system_report   = raw.func('void * pardox_get_system_report()');
    const pardox_get_quarantine_logs = raw.func('void * pardox_get_quarantine_logs()');
    const pardox_clear_quarantine    = raw.func('void pardox_clear_quarantine()');
    const pardox_sqlserver_config_ok = raw.func('int pardox_sqlserver_config_ok()');
    const pardox_version             = raw.func('const char * pardox_version()');

    // -------------------------------------------------------------------------
    // Gap 19: Data Contracts
    // -------------------------------------------------------------------------
    const pardox_validate_contract       = raw.func('int64_t pardox_validate_contract(void * mgr, const char * schema_json)');
    const pardox_contract_violation_count = raw.func('int64_t pardox_contract_violation_count()');

    // -------------------------------------------------------------------------
    // Gap 20: Time Travel
    // -------------------------------------------------------------------------
    const pardox_version_write  = raw.func('int64_t pardox_version_write(void * mgr, const char * path, const char * label, int64_t timestamp)');
    const pardox_version_read   = raw.func('void * pardox_version_read(const char * path, const char * label)');
    const pardox_version_list   = raw.func('void * pardox_version_list(const char * path)');
    const pardox_version_delete = raw.func('int64_t pardox_version_delete(const char * path, const char * label)');

    // -------------------------------------------------------------------------
    // Gap 21: Arrow Flight
    // -------------------------------------------------------------------------
    const pardox_flight_start    = raw.func('int64_t pardox_flight_start(int port)');
    const pardox_flight_register = raw.func('int64_t pardox_flight_register(const char * name, void * mgr)');
    const pardox_flight_stop     = raw.func('int pardox_flight_stop()');
    const pardox_flight_read     = raw.func('void * pardox_flight_read(const char * server, uint16_t port, const char * dataset)');

    // -------------------------------------------------------------------------
    // Gap 22: Cluster / Distributed (extended)
    // -------------------------------------------------------------------------
    const pardox_cluster_connect          = raw.func('int64_t pardox_cluster_connect(const char * worker_addresses_json)');
    const pardox_cluster_ping             = raw.func('int64_t pardox_cluster_ping(const char * worker_name)');
    const pardox_cluster_ping_all         = raw.func('int64_t pardox_cluster_ping_all()');
    const pardox_cluster_checkpoint       = raw.func('int64_t pardox_cluster_checkpoint(const char * path)');
    const pardox_cluster_free             = raw.func('void pardox_cluster_free()');
    const pardox_cluster_worker_start     = raw.func('int pardox_cluster_worker_start(uint16_t port)');
    const pardox_cluster_worker_stop      = raw.func('int pardox_cluster_worker_stop()');
    const pardox_cluster_worker_register  = raw.func('int pardox_cluster_worker_register(const char * name, const char * listen_addr)');
    const pardox_cluster_scatter          = raw.func('void * pardox_cluster_scatter(void * mgr, int partitions)');
    const pardox_cluster_scatter_resilient = raw.func('void * pardox_cluster_scatter_resilient(void * mgr, int partitions, int replication_factor)');
    const pardox_cluster_sql              = raw.func('void * pardox_cluster_sql(const char * sql_query)');
    const pardox_cluster_sql_resilient    = raw.func('void * pardox_cluster_sql_resilient(const char * sql_query)');

    // -------------------------------------------------------------------------
    // Gap 27: Query Planner (Lazy Pipeline extended)
    // -------------------------------------------------------------------------
    const pardox_lazy_scan_prdx = raw.func('void * pardox_lazy_scan_prdx(const char * path)');
    const pardox_lazy_optimize  = raw.func('void * pardox_lazy_optimize(void * frame)');
    const pardox_lazy_stats     = raw.func('const char * pardox_lazy_stats(void * frame)');

    // -------------------------------------------------------------------------
    // Gap 28: Linear Algebra
    // -------------------------------------------------------------------------
    const pardox_l2_normalize = raw.func('void * pardox_l2_normalize(void * mgr, const char * col_name)');
    const pardox_l1_normalize = raw.func('void * pardox_l1_normalize(void * mgr, const char * col_name)');
    const pardox_cosine_sim   = raw.func('double pardox_cosine_sim(void * left, const char * left_col, void * right, const char * right_col)');
    const pardox_matmul       = raw.func('void * pardox_matmul(void * left, const char * left_col, void * right, const char * right_col)');
    const pardox_pca          = raw.func('void * pardox_pca(void * mgr, const char * col_name, int n_components)');

    // -------------------------------------------------------------------------
    // Gap 29: REST Connector
    // -------------------------------------------------------------------------
    const pardox_read_rest = raw.func('void * pardox_read_rest(const char * url, const char * method, const char * headers_json)');

    // -------------------------------------------------------------------------
    // Arrow Ingestion
    // -------------------------------------------------------------------------
    const pardox_ingest_arrow_stream = raw.func('void * pardox_ingest_arrow_stream(const void * array_ptr, const void * schema_ptr)');

    // -------------------------------------------------------------------------
    // Wrap char*-returning functions with automatic decode + free
    // -------------------------------------------------------------------------

    /**
     * Read a null-terminated C string from an opaque pointer and free it.
     * Use ONLY for functions that return heap-allocated strings via CString::into_raw().
     * Functions: pardox_to_json_records, pardox_to_json_arrays, pardox_value_counts, pardox_unique.
     * @param {*}        ptr      koffi opaque pointer (void *)
     * @param {Function} freeFn   The pardox_free_string function
     * @returns {string|null}
     */
    function _readAndFreeStr(ptr, freeFn) {
        if (!ptr) return null;
        const str = koffi.decode(ptr, 'string');
        freeFn(ptr);
        return str;
    }

    /**
     * Read a null-terminated C string from a thread-local buffer pointer.
     * Use for functions that return via set_global_buffer() — DO NOT free these.
     * Functions: pardox_get_schema_json, pardox_manager_to_json, pardox_manager_to_ascii,
     *            pardox_manager_to_json_range, pardox_read_head_json.
     * @param {*} ptr  koffi opaque pointer (void *)
     * @returns {string|null}
     */
    function _readStr(ptr) {
        if (!ptr) return null;
        return koffi.decode(ptr, 'string');
        // DO NOT free — pointer is owned by Rust thread-local storage (set_global_buffer)
    }

    _lib = {
        // Memory
        pardox_free_manager,

        // Ingestion
        pardox_read_json_bytes,
        pardox_load_manager_csv,
        pardox_scan_sql,

        // Joins
        pardox_hash_join,

        // Slicing
        pardox_slice_manager,
        pardox_tail_manager,

        // Inspection — wrapped versions that return JS strings directly
        pardox_get_row_count: (mgr) => Number(pardox_get_row_count(mgr)),

        // Thread-local buffer — koffi auto-decodes 'const char *', no free needed
        pardox_get_schema_json:      (mgr)              => pardox_get_schema_json(mgr) || null,
        pardox_manager_to_json:      (mgr, limit)       => pardox_manager_to_json(mgr, limit) || null,
        pardox_manager_to_ascii:     (mgr, limit)       => pardox_manager_to_ascii(mgr, limit) || null,
        pardox_manager_to_json_range:(mgr, start, limit)=> pardox_manager_to_json_range(mgr, start, limit) || null,
        pardox_read_head_json:       (path, limit)      => pardox_read_head_json(path, limit) || null,

        // Casting & Mutation
        pardox_cast_column,
        pardox_add_column: (target, source, col) => Number(pardox_add_column(target, source, col)),

        // Cleaning
        pardox_fill_na: (mgr, col, val) => Number(pardox_fill_na(mgr, col, val)),
        pardox_round:   (mgr, col, dec) => Number(pardox_round(mgr, col, dec)),

        // Writers
        pardox_to_csv:    (mgr, path)  => Number(pardox_to_csv(mgr, path)),
        pardox_to_prdx:   (mgr, path)  => Number(pardox_to_prdx(mgr, path)),

        // Gap 15: Cloud Storage (S3/GCS/Azure)
        pardox_cloud_read_csv: (url, schema, config, creds) => pardox_cloud_read_csv(url, schema, config, creds),
        pardox_cloud_read_prdx: (url, limit, creds) => pardox_cloud_read_prdx(url, limit, creds),
        pardox_cloud_write_prdx: (mgr, url, creds) => Number(pardox_cloud_write_prdx(mgr, url, creds)),

        pardox_write_sql: (mgr, conn, table, mode, cols) =>
            Number(pardox_write_sql(mgr, conn, table, mode, cols)),
        pardox_execute_sql: (conn, query) => Number(pardox_execute_sql(conn, query)),

        pardox_write_sql_prdx: (prdxPath, conn, table, mode, conflictCols, batchRows) => {
            try {
                const fn = raw.func('int64_t pardox_write_sql_prdx(const char * prdx_path, const char * conn_str, const char * table_name, const char * mode, const char * conflict_cols_json, int64_t batch_rows)');
                return Number(fn(prdxPath, conn, table, mode, conflictCols, batchRows));
            } catch (e) {
                return -1;
            }
        },

        // MySQL
        pardox_read_mysql,
        pardox_write_mysql:   (mgr, conn, table, mode, cols) => Number(pardox_write_mysql(mgr, conn, table, mode, cols)),
        pardox_execute_mysql: (conn, query) => Number(pardox_execute_mysql(conn, query)),

        // SQL Server
        pardox_read_sqlserver,
        pardox_write_sqlserver:   (mgr, conn, table, mode, cols) => Number(pardox_write_sqlserver(mgr, conn, table, mode, cols)),
        pardox_execute_sqlserver: (conn, query) => Number(pardox_execute_sqlserver(conn, query)),

        // MongoDB
        pardox_read_mongodb,
        pardox_write_mongodb:   (mgr, conn, dbColl, mode) => Number(pardox_write_mongodb(mgr, conn, dbColl, mode)),
        pardox_execute_mongodb: (conn, db, cmdJson) => Number(pardox_execute_mongodb(conn, db, cmdJson)),

        // Arithmetic
        pardox_series_add,
        pardox_series_sub,
        pardox_series_mul,
        pardox_series_div,
        pardox_series_mod,

        // Filters
        pardox_filter_compare,
        pardox_filter_compare_scalar,
        pardox_apply_filter,

        // Aggregations
        pardox_agg_sum,
        pardox_agg_mean,
        pardox_agg_min,
        pardox_agg_max,
        pardox_agg_count,
        pardox_agg_std,

        // Native readers
        pardox_column_sum,
        pardox_query_sql,

        // Observer v0.3.1 — heap-allocated strings: decode + free via _readAndFreeStr
        pardox_to_json_records: (mgr)      => _readAndFreeStr(pardox_to_json_records(mgr), pardox_free_string),
        pardox_to_json_arrays:  (mgr)      => _readAndFreeStr(pardox_to_json_arrays(mgr),  pardox_free_string),
        pardox_value_counts:    (mgr, col) => _readAndFreeStr(pardox_value_counts(mgr, col), pardox_free_string),
        pardox_unique:          (mgr, col) => _readAndFreeStr(pardox_unique(mgr, col), pardox_free_string),

        // Native Math Foundation v0.3.1
        pardox_math_add,
        pardox_math_sub,
        pardox_math_stddev,
        pardox_math_minmax,
        pardox_sort_values: (mgr, col, descending) => pardox_sort_values(mgr, col, descending),
        pardox_gpu_sort,

        // Gap 1: GroupBy Aggregation
        pardox_groupby_agg,

        // Gap 2: String Functions
        pardox_str_upper,
        pardox_str_lower,
        pardox_str_contains,
        pardox_str_replace,
        pardox_str_trim,
        pardox_str_len,

        // Gap 2: Date Functions
        pardox_date_extract,
        pardox_date_format,
        pardox_date_add,
        pardox_date_diff,

        // Gap 3: Decimal Functions
        pardox_decimal_from_float,
        pardox_decimal_to_float,
        pardox_decimal_add,
        pardox_decimal_sub,
        pardox_decimal_mul_float,
        pardox_decimal_round,
        pardox_decimal_sum: (mgr, col) => Number(pardox_decimal_sum(mgr, col)),
        pardox_decimal_get_scale,

        // Gap 4: Window Functions
        pardox_row_number,
        pardox_rank,
        pardox_lag,
        pardox_lead,
        pardox_rolling_mean,

        // Gap 5: Lazy Pipeline
        pardox_lazy_scan_csv,
        pardox_lazy_select,
        pardox_lazy_filter,
        pardox_lazy_limit,
        pardox_lazy_collect,
        pardox_lazy_describe: (frame) => pardox_lazy_describe(frame) || null,
        pardox_lazy_free,

        // Gap 7: GPU Compute
        pardox_gpu_add,
        pardox_gpu_sub,
        pardox_gpu_mul,
        pardox_gpu_div,
        pardox_gpu_sqrt,
        pardox_gpu_exp,
        pardox_gpu_log,
        pardox_gpu_abs,

        // Gap 8: Pivot & Melt
        pardox_pivot_table,
        pardox_melt,

        // Gap 9: Time Series Fill
        pardox_ffill,
        pardox_bfill,
        pardox_interpolate,

        // Gap 10: Nested Data
        pardox_json_extract,
        pardox_explode,
        pardox_unnest,

        // Gap 11: Spill to Disk
        pardox_spill_to_disk: (mgr, path) => Number(pardox_spill_to_disk(mgr, path)),
        pardox_spill_from_disk,
        pardox_chunked_groupby,
        pardox_external_sort,
        pardox_memory_usage: (mgr) => Number(pardox_memory_usage(mgr)),

        // Gap 12: PRDX Loader
        pardox_load_manager_prdx,

        // Gap 16: Live Query
        pardox_live_query_start: (path, sql, pollMs) => pardox_live_query_start(path, sql, pollMs),
        pardox_live_query_version: (lq) => Number(pardox_live_query_version(lq)),
        pardox_live_query_take: (lq) => pardox_live_query_take(lq),
        pardox_live_query_free: (lq) => pardox_live_query_free(lq),

        // Gap 13: Streaming GroupBy & Aggregations over .prdx
        pardox_prdx_min,
        pardox_prdx_max,
        pardox_prdx_mean,
        pardox_prdx_count: (path) => Number(pardox_prdx_count(path)),
        pardox_groupby_agg_prdx,

        // Gap 16: Live Query
        pardox_live_query_start: (prdxPath, sqlQuery, pollMs) => pardox_live_query_start(prdxPath, sqlQuery, pollMs),
        pardox_live_query_version: (lq) => Number(pardox_live_query_version(lq)),
        pardox_live_query_take,
        pardox_live_query_free,

        // Gap 14: SQL Query over in-memory DataFrame
        pardox_sql_query,

        // Gap 18: Encryption (PRDX level)
        pardox_write_prdx_encrypted: (mgr, path, pwd) => Number(pardox_write_prdx_encrypted(mgr, path, pwd)),
        pardox_read_prdx_encrypted: (path, pwd) => pardox_read_prdx_encrypted(path, pwd),

        // Parquet
        pardox_to_parquet:            (mgr, path) => Number(pardox_to_parquet(mgr, path)),
        pardox_write_sharded_parquet: (mgr, dir, maxRows) => Number(pardox_write_sharded_parquet(mgr, dir, maxRows)),
        pardox_load_manager_parquet:  (path) => pardox_load_manager_parquet(path),

        // PRDX File Metadata (thread-local buffer — no free)
        pardox_read_schema:       (path) => pardox_read_schema(path) || null,
        pardox_inspect_file_lazy: (path, opts) => pardox_inspect_file_lazy(path, opts) || null,

        // Raw Buffer Ingestion
        pardox_ingest_buffer: (buf, len, layout, schema) => pardox_ingest_buffer(buf, len, layout, schema),

        // Bulk CSV to PRDX
        pardox_hyper_copy_v3: (src, dst, schema, config) => Number(pardox_hyper_copy_v3(src, dst, schema, config)),

        // Mainframe Import
        pardox_import_mainframe_dat: (path, schema) => pardox_import_mainframe_dat(path, schema),

        // Engine Utilities
        pardox_reset:               () => pardox_reset(),
        pardox_legacy_ping:         (name) => Number(pardox_legacy_ping(name)),
        pardox_get_system_report:   () => _readAndFreeStr(pardox_get_system_report(), pardox_free_string),
        pardox_get_quarantine_logs: () => _readAndFreeStr(pardox_get_quarantine_logs(), pardox_free_string),
        pardox_clear_quarantine:    () => pardox_clear_quarantine(),
        pardox_sqlserver_config_ok: () => pardox_sqlserver_config_ok(),
        pardox_version:             () => pardox_version() || null,

        // Gap 19: Data Contracts
        pardox_validate_contract:        (mgr, schemaJson) => Number(pardox_validate_contract(mgr, schemaJson)),
        pardox_contract_violation_count: () => Number(pardox_contract_violation_count()),

        // Gap 20: Time Travel
        pardox_version_write:  (mgr, path, label, ts) => Number(pardox_version_write(mgr, path, label, ts)),
        pardox_version_read:   (path, label) => pardox_version_read(path, label),
        pardox_version_list:   (path) => _readAndFreeStr(pardox_version_list(path), pardox_free_string),
        pardox_version_delete: (path, label) => Number(pardox_version_delete(path, label)),

        // Gap 21: Arrow Flight
        pardox_flight_start:    (port) => Number(pardox_flight_start(port)),
        pardox_flight_register: (name, mgr) => Number(pardox_flight_register(name, mgr)),
        pardox_flight_stop:     () => pardox_flight_stop(),
        pardox_flight_read:     (server, port, dataset) => pardox_flight_read(server, port, dataset),

        // Gap 22: Cluster (extended)
        pardox_cluster_connect:           (addrsJson) => Number(pardox_cluster_connect(addrsJson)),
        pardox_cluster_ping:              (name) => Number(pardox_cluster_ping(name)),
        pardox_cluster_ping_all:          () => Number(pardox_cluster_ping_all()),
        pardox_cluster_checkpoint:        (path) => Number(pardox_cluster_checkpoint(path)),
        pardox_cluster_free:              () => pardox_cluster_free(),
        pardox_cluster_worker_start:      (port) => pardox_cluster_worker_start(port),
        pardox_cluster_worker_stop:       () => pardox_cluster_worker_stop(),
        pardox_cluster_worker_register:   (name, addr) => pardox_cluster_worker_register(name, addr),
        pardox_cluster_scatter:           (mgr, parts) => _readAndFreeStr(pardox_cluster_scatter(mgr, parts), pardox_free_string),
        pardox_cluster_scatter_resilient: (mgr, parts, rep) => _readAndFreeStr(pardox_cluster_scatter_resilient(mgr, parts, rep), pardox_free_string),
        pardox_cluster_sql:               (sql) => pardox_cluster_sql(sql),
        pardox_cluster_sql_resilient:     (sql) => pardox_cluster_sql_resilient(sql),

        // Gap 27: Query Planner
        pardox_lazy_scan_prdx: (path) => pardox_lazy_scan_prdx(path),
        pardox_lazy_optimize:  (frame) => pardox_lazy_optimize(frame),
        pardox_lazy_stats:     (frame) => pardox_lazy_stats(frame) || null,

        // Gap 28: Linear Algebra
        pardox_l2_normalize: (mgr, col) => pardox_l2_normalize(mgr, col),
        pardox_l1_normalize: (mgr, col) => pardox_l1_normalize(mgr, col),
        pardox_cosine_sim:   (left, leftCol, right, rightCol) => pardox_cosine_sim(left, leftCol, right, rightCol),
        pardox_matmul:       (left, leftCol, right, rightCol) => pardox_matmul(left, leftCol, right, rightCol),
        pardox_pca:          (mgr, col, n) => pardox_pca(mgr, col, n),

        // Gap 29: REST Connector
        pardox_read_rest: (url, method, headersJson) => pardox_read_rest(url, method, headersJson),

        // Arrow Ingestion
        pardox_ingest_arrow_stream: (arrPtr, schemaPtr) => pardox_ingest_arrow_stream(arrPtr, schemaPtr),
    };

    return _lib;
}

module.exports = { getLib };
