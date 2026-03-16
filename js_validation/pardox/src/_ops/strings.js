'use strict';

const { getLib } = require('../ffi');

module.exports = {
    strUpper(col) {
        const lib = getLib();
        const ptr = lib.pardox_str_upper(this._ptr, col);
        if (!ptr) throw new Error('pardox_str_upper returned null.');
        return this.constructor._fromPtr(ptr);
    },

    strLower(col) {
        const lib = getLib();
        const ptr = lib.pardox_str_lower(this._ptr, col);
        if (!ptr) throw new Error('pardox_str_lower returned null.');
        return this.constructor._fromPtr(ptr);
    },

    strLen(col) {
        const lib = getLib();
        const ptr = lib.pardox_str_len(this._ptr, col);
        if (!ptr) throw new Error('pardox_str_len returned null.');
        return this.constructor._fromPtr(ptr);
    },

    strTrim(col) {
        const lib = getLib();
        const ptr = lib.pardox_str_trim(this._ptr, col);
        if (!ptr) throw new Error('pardox_str_trim returned null.');
        return this.constructor._fromPtr(ptr);
    },

    strReplace(col, oldStr, newStr) {
        const lib = getLib();
        const ptr = lib.pardox_str_replace(this._ptr, col, oldStr, newStr);
        if (!ptr) throw new Error('pardox_str_replace returned null.');
        return this.constructor._fromPtr(ptr);
    },

    strContains(col, pattern) {
        const lib = getLib();
        const ptr = lib.pardox_str_contains(this._ptr, col, pattern);
        if (!ptr) throw new Error('pardox_str_contains returned null.');
        return this.constructor._fromPtr(ptr);
    },
};
