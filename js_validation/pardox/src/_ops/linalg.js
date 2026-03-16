'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /**
     * L2-normalize a numeric column (unit vector).
     * @param {string} col
     * @returns {DataFrame}
     */
    linalgL2Normalize(col) {
        const lib = getLib();
        const ptr = lib.pardox_l2_normalize(this._ptr, col);
        if (!ptr) throw new Error('linalgL2Normalize returned null.');
        return this.constructor._fromPtr(ptr);
    },

    /**
     * L1-normalize a numeric column (sum-to-one).
     * @param {string} col
     * @returns {DataFrame}
     */
    linalgL1Normalize(col) {
        const lib = getLib();
        const ptr = lib.pardox_l1_normalize(this._ptr, col);
        if (!ptr) throw new Error('linalgL1Normalize returned null.');
        return this.constructor._fromPtr(ptr);
    },

    /**
     * Cosine similarity between two columns across two DataFrames.
     * @param {string}    colA   Column in this DataFrame.
     * @param {DataFrame} other  The other DataFrame.
     * @param {string}    colB   Column in other.
     * @returns {number}
     */
    linalgCosineSim(colA, other, colB) {
        const lib = getLib();
        return lib.pardox_cosine_sim(this._ptr, colA, other._ptr, colB);
    },

    /**
     * PCA — reduce a column to N principal components.
     * @param {string} col
     * @param {number} nComponents
     * @returns {DataFrame}
     */
    linalgPca(col, nComponents) {
        const lib = getLib();
        const ptr = lib.pardox_pca(this._ptr, col, nComponents);
        if (!ptr) throw new Error('linalgPca returned null.');
        return this.constructor._fromPtr(ptr);
    },

    /**
     * Matrix multiplication between columns of two DataFrames.
     * @param {string}    colA   Column in this DataFrame.
     * @param {DataFrame} other  Other DataFrame.
     * @param {string}    colB   Column in other.
     * @returns {DataFrame}
     */
    linalgMatmul(colA, other, colB) {
        const lib = getLib();
        const ptr = lib.pardox_matmul(this._ptr, colA, other._ptr, colB);
        if (!ptr) throw new Error('linalgMatmul returned null.');
        return this.constructor._fromPtr(ptr);
    },
};
