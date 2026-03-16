'use strict';

module.exports = function bindLinalg(_lib) {
    _lib.pardox_l2_normalize = _lib.raw.func('void * pardox_l2_normalize(void * mgr, const char * col_name)');
    _lib.pardox_l1_normalize = _lib.raw.func('void * pardox_l1_normalize(void * mgr, const char * col_name)');
    _lib.pardox_cosine_sim   = _lib.raw.func('double pardox_cosine_sim(void * left, const char * left_col, void * right, const char * right_col)');
    _lib.pardox_matmul       = _lib.raw.func('void * pardox_matmul(void * left, const char * left_col, void * right, const char * right_col)');
    _lib.pardox_pca          = _lib.raw.func('void * pardox_pca(void * mgr, const char * col_name, int n_components)');
};
