'use strict';

module.exports = function bindLazy(_lib) {
    _lib.pardox_lazy_scan_csv  = _lib.raw.func('void * pardox_lazy_scan_csv(const char * path, int delimiter, int has_header)');
    _lib.pardox_lazy_select    = _lib.raw.func('void * pardox_lazy_select(void * frame, const char * cols_json)');
    _lib.pardox_lazy_filter    = _lib.raw.func('void * pardox_lazy_filter(void * frame, const char * col, const char * op, double value)');
    _lib.pardox_lazy_limit     = _lib.raw.func('void * pardox_lazy_limit(void * frame, int64_t limit)');
    _lib.pardox_lazy_collect   = _lib.raw.func('void * pardox_lazy_collect(void * frame)');
    _lib._raw_pardox_lazy_describe = _lib.raw.func('const char * pardox_lazy_describe(void * frame)');
    _lib.pardox_lazy_free      = _lib.raw.func('void pardox_lazy_free(void * frame)');
    _lib.pardox_lazy_scan_prdx = _lib.raw.func('void * pardox_lazy_scan_prdx(const char * path)');
    _lib.pardox_lazy_optimize  = _lib.raw.func('void * pardox_lazy_optimize(void * frame)');
    _lib._raw_pardox_lazy_stats = _lib.raw.func('const char * pardox_lazy_stats(void * frame)');
};
