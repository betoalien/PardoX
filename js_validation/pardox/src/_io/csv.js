'use strict';

const { DataFrame } = require('../DataFrame');

/**
 * Default CSV reader configuration (comma-delimited, double-quoted, with header).
 */
const DEFAULT_CSV_CONFIG = JSON.stringify({
    delimiter:  44,
    quote_char: 34,
    has_header: true,
    chunk_size: 16 * 1024 * 1024,
});

/**
 * Read a CSV file into a PardoX DataFrame using the native Rust engine.
 * @param {string}      path    Path to the CSV file.
 * @param {Object|null} schema  Optional manual schema: { colName: 'Int64', ... }.
 * @returns {DataFrame}
 */
function read_csv(path, schema = null) {
    return DataFrame.read_csv(path, schema);
}

module.exports = { read_csv, DEFAULT_CSV_CONFIG };
