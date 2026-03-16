'use strict';

const { getLib } = require('../ffi');
const { DataFrame } = require('../DataFrame');

/**
 * Load a Parquet file into a PardoX DataFrame.
 * @param {string} path  Path to the .parquet file.
 * @returns {DataFrame}
 */
function readParquet(path) {
    const lib = getLib();
    const ptr = lib.pardox_load_manager_parquet(path);
    if (!ptr) throw new Error(`readParquet failed for: ${path}`);
    return new DataFrame(ptr);
}

module.exports = { readParquet };
