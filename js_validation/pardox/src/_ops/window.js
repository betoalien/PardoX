'use strict';

const { getLib } = require('../ffi');

module.exports = {
    windowRolling(col, windowSize) {
        const lib = getLib();
        const ptr = lib.pardox_rolling_mean(this._ptr, col, windowSize);
        if (!ptr) throw new Error('pardox_rolling_mean returned null.');
        return this.constructor._fromPtr(ptr);
    },

    windowLag(col, offset, fill = 0.0) {
        const lib = getLib();
        const ptr = lib.pardox_lag(this._ptr, col, offset, fill);
        if (!ptr) throw new Error('pardox_lag returned null.');
        return this.constructor._fromPtr(ptr);
    },

    windowLead(col, offset, fill = 0.0) {
        const lib = getLib();
        const ptr = lib.pardox_lead(this._ptr, col, offset, fill);
        if (!ptr) throw new Error('pardox_lead returned null.');
        return this.constructor._fromPtr(ptr);
    },

    windowRank(orderCol, ascending = true) {
        const lib = getLib();
        const ptr = lib.pardox_rank(this._ptr, orderCol, ascending ? 1 : 0);
        if (!ptr) throw new Error('pardox_rank returned null.');
        return this.constructor._fromPtr(ptr);
    },

    windowRowNumber(orderCol, ascending = true) {
        const lib = getLib();
        const ptr = lib.pardox_row_number(this._ptr, orderCol, ascending ? 1 : 0);
        if (!ptr) throw new Error('pardox_row_number returned null.');
        return this.constructor._fromPtr(ptr);
    },
};
