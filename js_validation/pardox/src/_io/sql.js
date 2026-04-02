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

/**
 * Stream SQL query results as an async generator of DataFrames.
 *
 * Reads large SQL tables in batches without loading the entire result into
 * memory. Only `batchSize` rows are held in RAM at any time.
 *
 * Requires the Rust core to export `pardox_scan_sql_cursor_open`.
 *
 * @param {string} connectionString  PostgreSQL connection URL.
 * @param {string} query             SQL query to stream.
 * @param {number} [batchSize=100000] Rows per chunk.
 * @yields {DataFrame} One DataFrame per batch.
 *
 * @example
 * const { queryToResults } = require('@pardox/pardox');
 * for await (const chunk of queryToResults(conn, 'SELECT * FROM orders', 50_000)) {
 *   const arrow = chunk.toArrow();
 * }
 */
async function* queryToResults(connectionString, query, batchSize = 100_000) {
    const lib = getLib();
    if (!lib.pardox_scan_sql_cursor_open) {
        throw new Error(
            "The current PardoX Core build does not export 'pardox_scan_sql_cursor_open'. " +
            "Re-compile the Rust core with Gap 30 (SQL Cursor API) enabled."
        );
    }

    const cursor = lib.pardox_scan_sql_cursor_open(connectionString, query, batchSize);
    if (!cursor) {
        throw new Error(
            'pardox_scan_sql_cursor_open returned null. ' +
            'Check the connection string and SQL syntax.'
        );
    }

    try {
        while (true) {
            const mgr = lib.pardox_scan_sql_cursor_fetch(cursor);
            if (!mgr) break;
            yield new DataFrame(mgr);
        }
    } finally {
        lib.pardox_scan_sql_cursor_close(cursor);
    }
}

/**
 * Export a SQL query directly to multiple Parquet files without loading into RAM.
 *
 * Streams query results in chunks and writes each chunk as a separate Parquet
 * file using the pattern provided.
 *
 * Requires the Rust core to export `pardox_scan_sql_to_parquet`.
 *
 * @param {string} connectionString  PostgreSQL connection URL.
 * @param {string} query             SQL query to export.
 * @param {string} outputPattern     File path pattern with `{i}` placeholder
 *                                   (e.g. `"/data/orders_chunk_{i}.parquet"`).
 * @param {number} [chunkSize=100000] Rows per Parquet file.
 * @returns {number} Total rows exported.
 *
 * @example
 * const { sqlToParquet } = require('@pardox/pardox');
 * const total = sqlToParquet(conn, 'SELECT * FROM orders', '/data/orders_{i}.parquet', 100_000);
 * console.log(`Exported ${total} rows`);
 */
function sqlToParquet(connectionString, query, outputPattern, chunkSize = 100_000) {
    const lib = getLib();
    if (!lib.pardox_scan_sql_to_parquet) {
        throw new Error(
            "The current PardoX Core build does not export 'pardox_scan_sql_to_parquet'. " +
            "Re-compile the Rust core with Gap 30 (SQL Cursor API) enabled."
        );
    }

    const total = lib.pardox_scan_sql_to_parquet(connectionString, query, outputPattern, chunkSize);
    if (total < 0) {
        throw new Error(`sqlToParquet failed with error code: ${total}`);
    }
    return total;
}

module.exports = { read_sql, executeSql, write_sql_prdx, queryToResults, sqlToParquet };
