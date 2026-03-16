'use strict';

module.exports = function bindStrings(_lib) {
    _lib.pardox_str_upper    = _lib.raw.func('void * pardox_str_upper(void * mgr, const char * col_name)');
    _lib.pardox_str_lower    = _lib.raw.func('void * pardox_str_lower(void * mgr, const char * col_name)');
    _lib.pardox_str_contains = _lib.raw.func('void * pardox_str_contains(void * mgr, const char * col_name, const char * pattern)');
    _lib.pardox_str_replace  = _lib.raw.func('void * pardox_str_replace(void * mgr, const char * col_name, const char * old_str, const char * new_str)');
    _lib.pardox_str_trim     = _lib.raw.func('void * pardox_str_trim(void * mgr, const char * col_name)');
    _lib.pardox_str_len      = _lib.raw.func('void * pardox_str_len(void * mgr, const char * col_name)');
};
