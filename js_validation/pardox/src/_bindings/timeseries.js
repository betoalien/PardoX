'use strict';

module.exports = function bindTimeseries(_lib) {
    _lib.pardox_ffill       = _lib.raw.func('void * pardox_ffill(void * mgr, const char * col)');
    _lib.pardox_bfill       = _lib.raw.func('void * pardox_bfill(void * mgr, const char * col)');
    _lib.pardox_interpolate = _lib.raw.func('void * pardox_interpolate(void * mgr, const char * col)');
};
