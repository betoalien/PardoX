'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /** Extract a value from a JSON column by key. */
    jsonExtract(col, key) {
        const lib = getLib();
        const ptr = lib.pardox_json_extract(this._ptr, col, key);
        if (!ptr) throw new Error('pardox_json_extract returned null.');
        return this.constructor._fromPtr(ptr);
    },

    /** Explode a list-like column, creating one row per element. */
    explode(col) {
        const lib = getLib();
        const ptr = lib.pardox_explode(this._ptr, col);
        if (!ptr) throw new Error('pardox_explode returned null.');
        return this.constructor._fromPtr(ptr);
    },

    /** Unnest a struct column, expanding its fields into separate columns. */
    unnest(col) {
        const lib = getLib();
        const ptr = lib.pardox_unnest(this._ptr, col);
        if (!ptr) throw new Error('pardox_unnest returned null.');
        return this.constructor._fromPtr(ptr);
    },
};
