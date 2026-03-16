'use strict';

module.exports = function bindMutation(_lib) {
    _lib.pardox_cast_column     = _lib.raw.func('int pardox_cast_column(void * mgr, const char * col_name, const char * target_type)');
    _lib._raw_pardox_add_column = _lib.raw.func('int64_t pardox_add_column(void * target, void * source, const char * col_name)');
    _lib._raw_pardox_fill_na    = _lib.raw.func('int64_t pardox_fill_na(void * mgr, const char * col, double val)');
    _lib._raw_pardox_round      = _lib.raw.func('int64_t pardox_round(void * mgr, const char * col, int decimals)');
};
