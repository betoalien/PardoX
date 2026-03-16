'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /**
     * Columnar addition: col_a + col_b → new DataFrame with "result_math_add".
     */
    add(colA, colB) {
        const lib = getLib();
        const ptr = lib.pardox_math_add(this._ptr, colA, colB);
        if (!ptr) throw new Error('pardox_math_add returned null.');
        return this.constructor._fromPtr(ptr);
    },

    /**
     * Columnar subtraction: col_a - col_b → new DataFrame with "result_math_sub".
     */
    sub(colA, colB) {
        const lib = getLib();
        const ptr = lib.pardox_math_sub(this._ptr, colA, colB);
        if (!ptr) throw new Error('pardox_math_sub returned null.');
        return this.constructor._fromPtr(ptr);
    },

    /**
     * Columnar multiplication: col_a * col_b → new DataFrame with "result_mul".
     */
    mul(colA, colB) {
        const lib = getLib();
        const ptr = lib.pardox_series_mul(this._ptr, colA, this._ptr, colB);
        if (!ptr) throw new Error('pardox_series_mul returned null.');
        return this.constructor._fromPtr(ptr);
    },

    /**
     * Native sample standard deviation of a column.
     */
    std(col) {
        const lib = getLib();
        return lib.pardox_math_stddev(this._ptr, col);
    },

    /**
     * Min-Max Scaler: normalizes column values to [0, 1].
     */
    minMaxScale(col) {
        const lib = getLib();
        const ptr = lib.pardox_math_minmax(this._ptr, col);
        if (!ptr) throw new Error('pardox_math_minmax returned null.');
        return this.constructor._fromPtr(ptr);
    },

    /**
     * Sort rows by the named column.
     * @param {string}  by        Column name to sort by.
     * @param {boolean} ascending Sort order (default true).
     * @param {boolean} gpu       Use GPU Bitonic sort pipeline.
     */
    sortValues(by, ascending = true, gpu = false) {
        const lib = getLib();
        let ptr;
        if (gpu) {
            ptr = lib.pardox_gpu_sort(this._ptr, by);
        } else {
            const descending = ascending ? 0 : 1;
            ptr = lib.pardox_sort_values(this._ptr, by, descending);
        }
        if (!ptr) throw new Error('Sort returned null.');
        return this.constructor._fromPtr(ptr);
    },
};
