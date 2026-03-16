'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /** Forward fill null values in a column. */
    ffill(col) {
        const lib = getLib();
        const ptr = lib.pardox_ffill(this._ptr, col);
        if (!ptr) throw new Error('pardox_ffill returned null.');
        return this.constructor._fromPtr(ptr);
    },

    /** Backward fill null values in a column. */
    bfill(col) {
        const lib = getLib();
        const ptr = lib.pardox_bfill(this._ptr, col);
        if (!ptr) throw new Error('pardox_bfill returned null.');
        return this.constructor._fromPtr(ptr);
    },

    /** Linear interpolation of null values in a column. */
    interpolate(col) {
        const lib = getLib();
        const ptr = lib.pardox_interpolate(this._ptr, col);
        if (!ptr) throw new Error('pardox_interpolate returned null.');
        return this.constructor._fromPtr(ptr);
    },
};
