'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /** Return a slice (new DataFrame) by row positions [start, end). */
    iloc(start, end) {
        return this.slice(start, end - start);
    },

    /**
     * Apply a boolean Series as a row filter.
     * @param {Series} mask  Boolean Series produced by a comparison operation.
     */
    filter(mask) {
        const { Series } = require('../Series');
        if (!(mask instanceof Series)) {
            throw new TypeError('filter() requires a PardoX Series mask.');
        }
        const lib = getLib();
        const resPtr = lib.pardox_apply_filter(
            this._ptr,
            mask._parentPtr(),
            mask.name
        );
        if (!resPtr) throw new Error('Filter operation returned null.');
        return this.constructor._fromPtr(resPtr);
    },
};
