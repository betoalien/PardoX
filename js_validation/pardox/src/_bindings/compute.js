'use strict';

module.exports = function bindCompute(_lib) {
    _lib.pardox_filter_compare = _lib.raw.func(
        'void * pardox_filter_compare(void * left, const char * left_col, void * right, const char * right_col, int op_code)'
    );
    _lib.pardox_filter_compare_scalar = _lib.raw.func(
        'void * pardox_filter_compare_scalar(void * mgr, const char * col, double val_f64, int64_t val_i64, int is_float, int op_code)'
    );
    _lib.pardox_apply_filter = _lib.raw.func('void * pardox_apply_filter(void * data, void * mask, const char * mask_col)');

    _lib.pardox_agg_sum   = _lib.raw.func('double pardox_agg_sum(void * mgr, const char * col)');
    _lib.pardox_agg_mean  = _lib.raw.func('double pardox_agg_mean(void * mgr, const char * col)');
    _lib.pardox_agg_min   = _lib.raw.func('double pardox_agg_min(void * mgr, const char * col)');
    _lib.pardox_agg_max   = _lib.raw.func('double pardox_agg_max(void * mgr, const char * col)');
    _lib.pardox_agg_count = _lib.raw.func('double pardox_agg_count(void * mgr, const char * col)');
    _lib.pardox_agg_std   = _lib.raw.func('double pardox_agg_std(void * mgr, const char * col)');

    _lib.pardox_series_add = _lib.raw.func('void * pardox_series_add(void * left, const char * left_col, void * right, const char * right_col)');
    _lib.pardox_series_sub = _lib.raw.func('void * pardox_series_sub(void * left, const char * left_col, void * right, const char * right_col)');
    _lib.pardox_series_mul = _lib.raw.func('void * pardox_series_mul(void * left, const char * left_col, void * right, const char * right_col)');
    _lib.pardox_series_div = _lib.raw.func('void * pardox_series_div(void * left, const char * left_col, void * right, const char * right_col)');
    _lib.pardox_series_mod = _lib.raw.func('void * pardox_series_mod(void * left, const char * left_col, void * right, const char * right_col)');
};
