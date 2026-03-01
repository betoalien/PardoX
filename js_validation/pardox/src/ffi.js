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

    // -------------------------------------------------------------------------
    // Warmup handshake
    // -------------------------------------------------------------------------
    try {
        pardox_init_engine();
    } catch (_) {
        // Non-fatal: older builds may not export this symbol
    }

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
        pardox_write_sql: (mgr, conn, table, mode, cols) =>
            Number(pardox_write_sql(mgr, conn, table, mode, cols)),
        pardox_execute_sql: (conn, query) => Number(pardox_execute_sql(conn, query)),

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
    };

    return _lib;
}

module.exports = { getLib };
