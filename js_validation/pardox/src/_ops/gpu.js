'use strict';

const { getLib } = require('../ffi');

module.exports = {
    gpuAdd(colA, colB) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_add(this._ptr, colA, colB);
        if (!ptr) throw new Error('pardox_gpu_add returned null.');
        return this.constructor._fromPtr(ptr);
    },

    gpuSub(colA, colB) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_sub(this._ptr, colA, colB);
        if (!ptr) throw new Error('pardox_gpu_sub returned null.');
        return this.constructor._fromPtr(ptr);
    },

    gpuMul(colA, colB) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_mul(this._ptr, colA, colB);
        if (!ptr) throw new Error('pardox_gpu_mul returned null.');
        return this.constructor._fromPtr(ptr);
    },

    gpuDiv(colA, colB) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_div(this._ptr, colA, colB);
        if (!ptr) throw new Error('pardox_gpu_div returned null.');
        return this.constructor._fromPtr(ptr);
    },

    gpuSqrt(col) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_sqrt(this._ptr, col);
        if (!ptr) throw new Error('pardox_gpu_sqrt returned null.');
        return this.constructor._fromPtr(ptr);
    },

    gpuExp(col) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_exp(this._ptr, col);
        if (!ptr) throw new Error('pardox_gpu_exp returned null.');
        return this.constructor._fromPtr(ptr);
    },

    gpuLog(col) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_log(this._ptr, col);
        if (!ptr) throw new Error('pardox_gpu_log returned null.');
        return this.constructor._fromPtr(ptr);
    },

    gpuAbs(col) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_abs(this._ptr, col);
        if (!ptr) throw new Error('pardox_gpu_abs returned null.');
        return this.constructor._fromPtr(ptr);
    },
};
