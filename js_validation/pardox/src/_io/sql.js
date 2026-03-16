'use strict';

const fs = require('fs');
const { getLib } = require('../ffi');
const { DataFrame } = require('../DataFrame');

/**
 * Execute a SQL query via the Rust native driver and return results as a DataFrame.
 * @param {string} connectionString  PostgreSQL connection URL.
 * @param {string} query             SQL query.
 * @returns {DataFrame}
 */
function read_sql(connectionString, query) {
    return DataFrame.read_sql(connectionString, query);
}

/**
 * Execute arbitrary DDL or DML directly via the Rust native driver.
 * @param {string} connectionString  PostgreSQL connection URL.
 * @param {string} query             SQL statement to execute.
 * @returns {number} Rows affected.
 */
function executeSql(connectionString, query) {
    const lib  = getLib();
    const rows = lib.pardox_execute_sql(connectionString, query);
    if (rows < 0) {
        const errorMap = {
            '-1':   'Invalid connection string',
            '-2':   'Invalid query string',
            '-100': 'Execute operation failed (check connection and SQL syntax)',
        };
        const msg = errorMap[String(rows)] ?? `Unknown error code: ${rows}`;
        throw new Error(`executeSql failed: ${msg}`);
    }
    return rows;
}

/**
 * Stream a PRDX file directly to PostgreSQL without loading into memory.
 * @param {string}   prdxPath          Path to the .prdx file.
 * @param {string}   connectionString  PostgreSQL connection URL.
 * @param {string}   tableName         Target table name.
 * @param {string}   mode              'append' or 'replace'. Default 'append'.
 * @param {string[]} conflictCols      Columns for conflict resolution.
 * @param {number}   batchRows         Rows per batch. Default 1,000,000.
 * @returns {number} Rows written or negative error code.
 */
function write_sql_prdx(prdxPath, connectionString, tableName, mode = 'append', conflictCols = [], batchRows = 1000000) {
    if (!fs.existsSync(prdxPath)) {
        throw new Error(`PRDX file not found: ${prdxPath}`);
    }
    const lib = getLib();
    const rows = lib.pardox_write_sql_prdx(prdxPath, connectionString, tableName, mode, JSON.stringify(conflictCols), batchRows);
    if (rows < 0) {
        const errorMap = {
            '-1':  'Invalid PRDX path',
            '-2':  'Invalid connection string',
            '-3':  'Invalid table name',
            '-4':  'Invalid mode',
            '-5':  'Invalid conflict columns JSON',
            '-10': 'Failed to open PRDX file',
            '-20': 'PostgreSQL connection failed',
        };
        const msg = errorMap[String(rows)] ?? `Unknown error code: ${rows}`;
        throw new Error(`write_sql_prdx failed: ${msg}`);
    }
    return rows;
}

module.exports = { read_sql, executeSql, write_sql_prdx };
