'use strict';

module.exports = function bindPrdxIo(_lib) {
    _lib.pardox_load_manager_prdx    = _lib.raw.func('void * pardox_load_manager_prdx(const char * path, int64_t limit)');
    _lib._raw_pardox_to_prdx         = _lib.raw.func('int64_t pardox_to_prdx(void * mgr, const char * path)');
    _lib.pardox_load_manager_parquet = _lib.raw.func('void * pardox_load_manager_parquet(const char * path)');
    _lib._raw_pardox_read_head_json  = _lib.raw.func('const char * pardox_read_head_json(const char * path, size_t limit)');
    _lib.pardox_column_sum           = _lib.raw.func('double pardox_column_sum(const char * path, const char * col)');
    _lib.pardox_query_sql            = _lib.raw.func('const char * pardox_query_sql(const char * path, const char * query)');
    _lib.pardox_prdx_min             = _lib.raw.func('double pardox_prdx_min(const char * path, const char * col)');
    _lib.pardox_prdx_max             = _lib.raw.func('double pardox_prdx_max(const char * path, const char * col)');
    _lib.pardox_prdx_mean            = _lib.raw.func('double pardox_prdx_mean(const char * path, const char * col)');
    _lib._raw_pardox_prdx_count      = _lib.raw.func('int64_t pardox_prdx_count(const char * path)');
    _lib._raw_pardox_read_schema     = _lib.raw.func('const char * pardox_read_schema(const char * path)');
};
