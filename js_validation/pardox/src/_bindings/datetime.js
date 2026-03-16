'use strict';

module.exports = function bindDatetime(_lib) {
    _lib.pardox_date_extract = _lib.raw.func('void * pardox_date_extract(void * mgr, const char * col_name, const char * part)');
    _lib.pardox_date_format  = _lib.raw.func('void * pardox_date_format(void * mgr, const char * col_name, const char * fmt)');
    _lib.pardox_date_add     = _lib.raw.func('void * pardox_date_add(void * mgr, const char * col_name, int64_t days)');
    _lib.pardox_date_diff    = _lib.raw.func('void * pardox_date_diff(void * mgr, const char * col_a, const char * col_b, const char * unit)');
};
