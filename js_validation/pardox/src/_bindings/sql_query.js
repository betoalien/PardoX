'use strict';

module.exports = function bindSqlQuery(_lib) {
    _lib.pardox_sql_query         = _lib.raw.func('void * pardox_sql_query(void * mgr, const char * sql)');
    _lib._raw_pardox_execute_sql  = _lib.raw.func('int64_t pardox_execute_sql(const char * conn_str, const char * query)');
    _lib._raw_pardox_write_sql    = _lib.raw.func('int64_t pardox_write_sql(void * mgr, const char * conn_str, const char * table, const char * mode, const char * conflict_cols_json)');
    _lib.pardox_sqlserver_config_ok = _lib.raw.func('int pardox_sqlserver_config_ok()');

    // Gap 30 — SQL Cursor API (streaming batch reads from SQL databases)
    try {
        _lib._raw_pardox_scan_sql_cursor_open   = _lib.raw.func('void * pardox_scan_sql_cursor_open(const char * conn_str, const char * query, size_t chunk_size)');
        _lib._raw_pardox_scan_sql_cursor_fetch  = _lib.raw.func('void * pardox_scan_sql_cursor_fetch(void * cursor)');
        _lib._raw_pardox_scan_sql_cursor_offset = _lib.raw.func('size_t pardox_scan_sql_cursor_offset(void * cursor)');
        _lib._raw_pardox_scan_sql_cursor_close  = _lib.raw.func('void pardox_scan_sql_cursor_close(void * cursor)');
        _lib._raw_pardox_scan_sql_to_parquet    = _lib.raw.func('int64_t pardox_scan_sql_to_parquet(const char * conn_str, const char * query, const char * output_pattern, int64_t chunk_size)');
    } catch (_) {
        // Functions not present in this build — bindings skipped
    }
};
