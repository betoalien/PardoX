'use strict';

const { getLib } = require('../ffi');
const { DataFrame } = require('../DataFrame');

/**
 * Bulk-convert CSV files matching a glob pattern to a .prdx file.
 * @param {string} srcPattern  Glob pattern for source CSV files.
 * @param {string} outPath     Destination .prdx file path.
 * @param {Object} schema      Optional schema override.
 * @param {Object} config      Optional CSV config.
 * @returns {number} Rows copied.
 */
function hyperCopy(srcPattern, outPath, schema = {}, config = {}) {
    const lib           = getLib();
    const defaultConfig = { delimiter: 44, quote_char: 34, has_header: true };
    const result        = lib.pardox_hyper_copy_v3(
        srcPattern,
        outPath,
        JSON.stringify(Object.keys(schema).length ? schema : {}),
        JSON.stringify(Object.keys(config).length ? config : defaultConfig)
    );
    if (result < 0) throw new Error(`hyperCopy failed with code: ${result}`);
    return result;
}

/**
 * Import a mainframe fixed-width .dat file into a DataFrame.
 * @param {string} path        Path to the .dat file.
 * @param {Object} schemaJson  Schema descriptor.
 * @returns {DataFrame}
 */
function readDat(path, schemaJson = {}) {
    const lib = getLib();
    const ptr = lib.pardox_import_mainframe_dat(path, JSON.stringify(schemaJson));
    if (!ptr) throw new Error(`readDat failed for: ${path}`);
    return new DataFrame(ptr);
}

/**
 * Ingest a raw memory buffer into a DataFrame.
 * @param {*}      bufferPtr   Pointer to the buffer.
 * @param {number} bufferLen   Length of the buffer in bytes.
 * @param {Object} layoutJson  Buffer layout descriptor.
 * @param {Object} schemaJson  Schema descriptor.
 * @returns {DataFrame}
 */
function ingestBuffer(bufferPtr, bufferLen, layoutJson = {}, schemaJson = {}) {
    const lib = getLib();
    const ptr = lib.pardox_ingest_buffer(bufferPtr, bufferLen, JSON.stringify(layoutJson), JSON.stringify(schemaJson));
    if (!ptr) throw new Error('ingestBuffer failed (Rust returned null).');
    return new DataFrame(ptr);
}

module.exports = { hyperCopy, readDat, ingestBuffer };
