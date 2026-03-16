'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /**
     * Write a versioned snapshot of this DataFrame to disk.
     * @param {string} path       Base directory path.
     * @param {string} label      Version label.
     * @param {number} timestamp  Optional Unix timestamp (0 = current time).
     * @returns {number} Rows written.
     */
    versionWrite(path, label, timestamp = 0) {
        const lib = getLib();
        const result = lib.pardox_version_write(this._ptr, path, label, timestamp);
        if (result < 0) throw new Error(`versionWrite failed with code: ${result}`);
        return result;
    },
};
