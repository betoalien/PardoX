'use strict';

module.exports = function bindGroupby(_lib) {
    _lib.pardox_groupby_agg      = _lib.raw.func('void * pardox_groupby_agg(void * mgr, const char * group_cols_json, const char * agg_json)');
    _lib.pardox_groupby_agg_prdx = _lib.raw.func('void * pardox_groupby_agg_prdx(const char * path, const char * group_cols_json, const char * agg_json)');
    _lib.pardox_chunked_groupby  = _lib.raw.func('void * pardox_chunked_groupby(void * mgr, const char * group_col, const char * agg_json, int64_t chunk_size)');
};
