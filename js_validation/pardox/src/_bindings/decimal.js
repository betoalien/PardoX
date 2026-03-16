'use strict';

module.exports = function bindDecimal(_lib) {
    _lib.pardox_decimal_from_float = _lib.raw.func('void * pardox_decimal_from_float(void * mgr, const char * col_name, uint8_t scale)');
    _lib.pardox_decimal_to_float   = _lib.raw.func('void * pardox_decimal_to_float(void * mgr, const char * col_name)');
    _lib.pardox_decimal_add        = _lib.raw.func('void * pardox_decimal_add(void * mgr, const char * col_a, const char * col_b)');
    _lib.pardox_decimal_sub        = _lib.raw.func('void * pardox_decimal_sub(void * mgr, const char * col_a, const char * col_b)');
    _lib.pardox_decimal_mul_float  = _lib.raw.func('void * pardox_decimal_mul_float(void * mgr, const char * col_name, double factor)');
    _lib.pardox_decimal_round      = _lib.raw.func('void * pardox_decimal_round(void * mgr, const char * col_name, uint8_t scale)');
    _lib._raw_pardox_decimal_sum   = _lib.raw.func('double pardox_decimal_sum(void * mgr, const char * col_name)');
    _lib.pardox_decimal_get_scale  = _lib.raw.func('int pardox_decimal_get_scale(void * mgr, const char * col_name)');
};
