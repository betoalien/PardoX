'use strict';

module.exports = function bindInspection(_lib) {
    _lib._raw_pardox_get_row_count        = _lib.raw.func('int64_t pardox_get_row_count(void * mgr)');
    _lib._raw_pardox_get_schema_json      = _lib.raw.func('const char * pardox_get_schema_json(void * mgr)');
    _lib._raw_pardox_manager_to_json      = _lib.raw.func('const char * pardox_manager_to_json(void * mgr, size_t limit)');
    _lib._raw_pardox_manager_to_ascii     = _lib.raw.func('const char * pardox_manager_to_ascii(void * mgr, size_t limit)');
    _lib._raw_pardox_manager_to_json_range = _lib.raw.func('const char * pardox_manager_to_json_range(void * mgr, size_t start, size_t limit)');
    _lib.pardox_slice_manager             = _lib.raw.func('void * pardox_slice_manager(void * mgr, size_t start, size_t len)');
    _lib.pardox_tail_manager              = _lib.raw.func('void * pardox_tail_manager(void * mgr, size_t n)');
    _lib._raw_pardox_get_f64_buffer       = _lib.raw.func('void * pardox_get_f64_buffer(void * mgr, const char * col, size_t * out_len)');
};
