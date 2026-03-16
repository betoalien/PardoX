'use strict';

module.exports = function bindMathOps(_lib) {
    _lib.pardox_math_add    = _lib.raw.func('void * pardox_math_add(void * mgr, const char * col_a, const char * col_b)');
    _lib.pardox_math_sub    = _lib.raw.func('void * pardox_math_sub(void * mgr, const char * col_a, const char * col_b)');
    _lib.pardox_math_stddev = _lib.raw.func('double pardox_math_stddev(void * mgr, const char * col_name)');
    _lib.pardox_math_minmax = _lib.raw.func('void * pardox_math_minmax(void * mgr, const char * col_name)');
    _lib.pardox_sort_values = _lib.raw.func('void * pardox_sort_values(void * mgr, const char * col_name, int descending)');
    _lib.pardox_gpu_sort    = _lib.raw.func('void * pardox_gpu_sort(void * mgr, const char * col_name)');
};
