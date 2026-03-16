'use strict';

module.exports = function bindWindow(_lib) {
    _lib.pardox_row_number   = _lib.raw.func('void * pardox_row_number(void * mgr, const char * order_col, int ascending)');
    _lib.pardox_rank         = _lib.raw.func('void * pardox_rank(void * mgr, const char * order_col, int ascending)');
    _lib.pardox_lag          = _lib.raw.func('void * pardox_lag(void * mgr, const char * col_name, int64_t offset, double fill)');
    _lib.pardox_lead         = _lib.raw.func('void * pardox_lead(void * mgr, const char * col_name, int64_t offset, double fill)');
    _lib.pardox_rolling_mean = _lib.raw.func('void * pardox_rolling_mean(void * mgr, const char * col_name, int64_t window)');
};
