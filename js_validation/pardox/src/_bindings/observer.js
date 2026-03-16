'use strict';

module.exports = function bindObserver(_lib) {
    _lib._raw_pardox_to_json_records = _lib.raw.func('void * pardox_to_json_records(void * mgr)');
    _lib._raw_pardox_to_json_arrays  = _lib.raw.func('void * pardox_to_json_arrays(void * mgr)');
    _lib._raw_pardox_value_counts    = _lib.raw.func('void * pardox_value_counts(void * mgr, const char * col_name)');
    _lib._raw_pardox_unique          = _lib.raw.func('void * pardox_unique(void * mgr, const char * col_name)');
};
