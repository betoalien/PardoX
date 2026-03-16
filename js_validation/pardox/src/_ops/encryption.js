'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /**
     * Write the DataFrame to an AES-encrypted .prdx file.
     * @param {string} path
     * @param {string} password
     * @returns {number}
     */
    toPrdxEncrypted(path, password) {
        const lib = getLib();
        const result = lib.pardox_write_prdx_encrypted(this._ptr, path, password);
        if (result < 0) throw new Error(`toPrdxEncrypted failed with code: ${result}`);
        return result;
    },
};
