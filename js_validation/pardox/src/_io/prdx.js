'use strict';

const fs = require('fs');
const { getLib } = require('../ffi');
const { DataFrame } = require('../DataFrame');

/**
 * Read a native PardoX (.prdx) file and return a preview as a JS array.
 * @param {string} path   Path to the .prdx file.
 * @param {number} limit  Maximum number of rows to read.
 * @returns {Object[]}
 */
function read_prdx(path, limit = 100) {
    if (!fs.existsSync(path)) {
        throw new Error(`File not found: ${path}`);
    }
    const lib  = getLib();
    const json = lib.pardox_read_head_json(path, limit);
    if (json === null) {
        throw new Error(`Failed to read PRDX file (Rust returned null): ${path}`);
    }
    try {
        return JSON.parse(json);
    } catch (e) {
        throw new Error(`Failed to decode PRDX data as JSON: ${e.message}`);
    }
}

/**
 * Read the schema of a .prdx file without loading data.
 * @param {string} path
 * @returns {Object}
 */
function readPrdxSchema(path) {
    const lib    = getLib();
    const result = lib.pardox_read_schema(path);
    return result ? JSON.parse(result) : {};
}

/**
 * Lazily inspect a .prdx file and return metadata.
 * @param {string} path
 * @param {Object} options
 * @returns {Object}
 */
function inspectPrdx(path, options = {}) {
    const lib    = getLib();
    const result = lib.pardox_inspect_file_lazy(path, JSON.stringify(options));
    return result ? JSON.parse(result) : {};
}

/**
 * Execute a SQL query directly on a .prdx file (no full load).
 * @param {string} path
 * @param {string} query
 * @returns {string|null} JSON string of results.
 */
function querySqlPrdx(path, query) {
    const lib = getLib();
    return lib.pardox_query_sql(path, query);
}

/**
 * Sum a column in a .prdx file without fully loading it.
 * @param {string} path
 * @param {string} col
 * @returns {number}
 */
function prdxColumnSum(path, col) {
    const lib = getLib();
    return lib.pardox_column_sum(path, col);
}

/**
 * Get the minimum value of a column in a .prdx file.
 * @param {string} path
 * @param {string} col
 * @returns {number}
 */
function prdxMin(path, col) {
    const lib = getLib();
    return lib.pardox_prdx_min(path, col);
}

/**
 * Get the maximum value of a column in a .prdx file.
 * @param {string} path
 * @param {string} col
 * @returns {number}
 */
function prdxMax(path, col) {
    const lib = getLib();
    return lib.pardox_prdx_max(path, col);
}

/**
 * Get the mean value of a column in a .prdx file.
 * @param {string} path
 * @param {string} col
 * @returns {number}
 */
function prdxMean(path, col) {
    const lib = getLib();
    return lib.pardox_prdx_mean(path, col);
}

/**
 * Get the row count of a .prdx file.
 * @param {string} path
 * @returns {number}
 */
function prdxCount(path) {
    const lib = getLib();
    return lib.pardox_prdx_count(path);
}

/**
 * Run a streaming GroupBy aggregation on a .prdx file.
 * @param {string}   path
 * @param {string[]} groupCols
 * @param {Object}   agg
 * @returns {DataFrame}
 */
function prdxGroupby(path, groupCols, agg) {
    const lib = getLib();
    const ptr = lib.pardox_groupby_agg_prdx(path, JSON.stringify(groupCols), JSON.stringify(agg));
    if (!ptr) throw new Error(`prdxGroupby failed for: ${path}`);
    return new DataFrame(ptr);
}

module.exports = {
    read_prdx,
    readPrdxSchema,
    inspectPrdx,
    querySqlPrdx,
    prdxColumnSum,
    prdxMin,
    prdxMax,
    prdxMean,
    prdxCount,
    prdxGroupby,
};
