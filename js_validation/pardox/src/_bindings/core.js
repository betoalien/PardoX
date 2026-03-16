'use strict';

module.exports = function bindCore(_lib) {
    _lib.pardox_init_engine    = _lib.raw.func('void pardox_init_engine()');
    _lib.pardox_free_manager   = _lib.raw.func('void pardox_free_manager(void * mgr)');
    _lib.pardox_free_string    = _lib.raw.func('void pardox_free_string(void * ptr)');
    _lib.pardox_read_json_bytes = _lib.raw.func('void * pardox_read_json_bytes(const char * bytes, size_t len)');
    _lib.pardox_load_manager_csv = _lib.raw.func('void * pardox_load_manager_csv(const char * path, const char * schema, const char * config)');
    _lib.pardox_scan_sql       = _lib.raw.func('void * pardox_scan_sql(const char * conn_str, const char * query)');
    _lib.pardox_hash_join      = _lib.raw.func('void * pardox_hash_join(void * left, void * right, const char * left_key, const char * right_key)');
    _lib.pardox_ingest_arrow_stream = _lib.raw.func('void * pardox_ingest_arrow_stream(const void * array_ptr, const void * schema_ptr)');
};
