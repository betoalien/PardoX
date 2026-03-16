'use strict';

const { getLib } = require('../ffi');
const { DataFrame } = require('../DataFrame');

/**
 * Read an AES-encrypted .prdx file into a DataFrame.
 * @param {string} path      Path to the encrypted .prdx file.
 * @param {string} password  Decryption password.
 * @returns {DataFrame}
 */
function readPrdxEncrypted(path, password) {
    const lib = getLib();
    const ptr = lib.pardox_read_prdx_encrypted(path, password);
    if (!ptr) throw new Error(`readPrdxEncrypted failed for: ${path}`);
    return new DataFrame(ptr);
}

module.exports = { readPrdxEncrypted };
