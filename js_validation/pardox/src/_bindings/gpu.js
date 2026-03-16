'use strict';

module.exports = function bindGpu(_lib) {
    _lib.pardox_gpu_add  = _lib.raw.func('void * pardox_gpu_add(void * mgr, const char * col_a, const char * col_b)');
    _lib.pardox_gpu_sub  = _lib.raw.func('void * pardox_gpu_sub(void * mgr, const char * col_a, const char * col_b)');
    _lib.pardox_gpu_mul  = _lib.raw.func('void * pardox_gpu_mul(void * mgr, const char * col_a, const char * col_b)');
    _lib.pardox_gpu_div  = _lib.raw.func('void * pardox_gpu_div(void * mgr, const char * col_a, const char * col_b)');
    _lib.pardox_gpu_sqrt = _lib.raw.func('void * pardox_gpu_sqrt(void * mgr, const char * col)');
    _lib.pardox_gpu_exp  = _lib.raw.func('void * pardox_gpu_exp(void * mgr, const char * col)');
    _lib.pardox_gpu_log  = _lib.raw.func('void * pardox_gpu_log(void * mgr, const char * col)');
    _lib.pardox_gpu_abs  = _lib.raw.func('void * pardox_gpu_abs(void * mgr, const char * col)');
};
