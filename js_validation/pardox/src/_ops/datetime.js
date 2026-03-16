'use strict';

const { getLib } = require('../ffi');

module.exports = {
    dateExtract(col, part) {
        const lib = getLib();
        const ptr = lib.pardox_date_extract(this._ptr, col, part);
        if (!ptr) throw new Error('pardox_date_extract returned null.');
        return this.constructor._fromPtr(ptr);
    },

    dateFormat(col, fmt) {
        const lib = getLib();
        const ptr = lib.pardox_date_format(this._ptr, col, fmt);
        if (!ptr) throw new Error('pardox_date_format returned null.');
        return this.constructor._fromPtr(ptr);
    },

    dateAdd(col, days) {
        const lib = getLib();
        const ptr = lib.pardox_date_add(this._ptr, col, days);
        if (!ptr) throw new Error('pardox_date_add returned null.');
        return this.constructor._fromPtr(ptr);
    },

    dateDiff(colA, colB, unit = 'days') {
        const lib = getLib();
        const ptr = lib.pardox_date_diff(this._ptr, colA, colB, unit);
        if (!ptr) throw new Error('pardox_date_diff returned null.');
        return this.constructor._fromPtr(ptr);
    },
};
