'use strict';

const { getLib } = require('../ffi');

module.exports = {
    decimalFromFloat(col, scale) {
        const lib = getLib();
        const ptr = lib.pardox_decimal_from_float(this._ptr, col, scale);
        if (!ptr) throw new Error('pardox_decimal_from_float returned null.');
        return this.constructor._fromPtr(ptr);
    },

    decimalToFloat(col) {
        const lib = getLib();
        const ptr = lib.pardox_decimal_to_float(this._ptr, col);
        if (!ptr) throw new Error('pardox_decimal_to_float returned null.');
        return this.constructor._fromPtr(ptr);
    },

    decimalRound(col, scale) {
        const lib = getLib();
        const ptr = lib.pardox_decimal_round(this._ptr, col, scale);
        if (!ptr) throw new Error('pardox_decimal_round returned null.');
        return this.constructor._fromPtr(ptr);
    },

    decimalMulFloat(col, factor) {
        const lib = getLib();
        const ptr = lib.pardox_decimal_mul_float(this._ptr, col, factor);
        if (!ptr) throw new Error('pardox_decimal_mul_float returned null.');
        return this.constructor._fromPtr(ptr);
    },

    decimalSum(col) {
        const lib = getLib();
        return lib.pardox_decimal_sum(this._ptr, col);
    },
};
