'use strict';

module.exports = function bindReshape(_lib) {
    _lib.pardox_pivot_table = _lib.raw.func('void * pardox_pivot_table(void * mgr, const char * index, const char * columns, const char * values, const char * agg_func)');
    _lib.pardox_melt        = _lib.raw.func('void * pardox_melt(void * mgr, const char * id_vars_json, const char * value_vars_json, const char * var_name, const char * value_name)');
    _lib.pardox_explode     = _lib.raw.func('void * pardox_explode(void * mgr, const char * col)');
    _lib.pardox_unnest      = _lib.raw.func('void * pardox_unnest(void * mgr, const char * col)');
    _lib.pardox_json_extract = _lib.raw.func('void * pardox_json_extract(void * mgr, const char * col, const char * key)');
};
