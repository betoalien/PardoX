'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /** Print N rows as an ASCII table to stdout. */
    show(n = 10) {
        const lib = getLib();
        const ascii = lib.pardox_manager_to_ascii(this._ptr, n);
        if (ascii) console.log('\n' + ascii);
    },

    /** Return a new DataFrame with the first N rows. */
    head(n = 5) {
        return this.slice(0, n);
    },

    /** Return a new DataFrame with the last N rows. */
    tail(n = 5) {
        const lib = getLib();
        const newPtr = lib.pardox_tail_manager(this._ptr, n);
        if (!newPtr) throw new Error('tail() failed (Rust returned null).');
        return this.constructor._fromPtr(newPtr);
    },
};
