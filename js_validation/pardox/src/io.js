'use strict';

/**
 * PardoX IO module for Node.js
 *
 * Static reader functions that load data into DataFrames.
 * Mirrors Python's pardox/io.py and PHP's IO.php.
 */

const fs = require('fs');
const { getLib } = require('./ffi');
const { DataFrame } = require('./DataFrame');

/**
 * Default CSV reader configuration (comma-delimited, double-quoted, with header).
 */
const DEFAULT_CSV_CONFIG = JSON.stringify({
    delimiter:  44,             // ',' ASCII 44
    quote_char: 34,             // '"' ASCII 34
    has_header: true,
    chunk_size: 16 * 1024 * 1024,  // 16 MB
});

/**
 * Read a CSV file into a PardoX DataFrame using the native Rust engine.
 *
 * @param {string}      path    Path to the CSV file.
 * @param {Object|null} schema  Optional manual schema: { colName: 'Int64', ... }.
 *                              Pass null to trigger automatic type inference.
 * @returns {DataFrame}
 */
function read_csv(path, schema = null) {
    return DataFrame.read_csv(path, schema);
}

/**
 * Execute a SQL query via the Rust native driver and return the results.
 *
 * Bypasses Node.js completely: Rust connects to the database, executes the
 * query, and fills memory buffers before handing back a pointer.
 *
 * @param {string} connectionString  PostgreSQL connection URL.
 * @param {string} query             SQL query.
 * @returns {DataFrame}
 */
function read_sql(connectionString, query) {
    return DataFrame.read_sql(connectionString, query);
}

/**
 * Read a native PardoX (.prdx) file and return a preview as a JS array.
 *
 * @param {string} path   Path to the .prdx file.
 * @param {number} limit  Maximum number of rows to read.
 * @returns {Object[]}    Array of row objects.
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
 * Execute arbitrary DDL or DML directly via the Rust native driver.
 *
 * Bypasses Node.js completely: Rust connects to the database and runs the
 * statement. Useful for CREATE TABLE, DROP TABLE, UPDATE, DELETE, etc.
 *
 * @param {string} connectionString  PostgreSQL connection URL.
 * @param {string} query             SQL statement to execute.
 * @returns {number}  Number of rows affected (0 for DDL statements).
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
 * Read a MySQL table/query into a PardoX DataFrame using the native Rust driver.
 *
 * @param {string} connectionString  MySQL URL (e.g. "mysql://user:pass@host:3306/db").
 * @param {string} query             SQL query to execute.
 * @returns {DataFrame}
 */
function read_mysql(connectionString, query) {
    return DataFrame.read_mysql(connectionString, query);
}

/**
 * Execute arbitrary DDL or DML directly against MySQL via the Rust native driver.
 *
 * @param {string} connectionString  MySQL URL.
 * @param {string} query             SQL statement to execute.
 * @returns {number}  Number of rows affected.
 */
function execute_mysql(connectionString, query) {
    const lib  = getLib();
    const rows = lib.pardox_execute_mysql(connectionString, query);
    if (rows < 0) {
        throw new Error(`execute_mysql failed with error code: ${rows}`);
    }
    return rows;
}

/**
 * Read from SQL Server into a PardoX DataFrame using the native Rust driver (tiberius).
 *
 * @param {string} connectionString  ADO.NET connection string.
 * @param {string} query             SQL query to execute.
 * @returns {DataFrame}
 */
function read_sqlserver(connectionString, query) {
    return DataFrame.read_sqlserver(connectionString, query);
}

/**
 * Execute arbitrary DDL or DML directly against SQL Server via the Rust native driver.
 *
 * @param {string} connectionString  ADO.NET connection string.
 * @param {string} query             SQL statement to execute.
 * @returns {number}  Number of rows affected.
 */
function execute_sqlserver(connectionString, query) {
    const lib  = getLib();
    const rows = lib.pardox_execute_sqlserver(connectionString, query);
    if (rows < 0) {
        throw new Error(`execute_sqlserver failed with error code: ${rows}`);
    }
    return rows;
}

/**
 * Read a MongoDB collection into a PardoX DataFrame using the native Rust driver.
 *
 * @param {string} connectionString   MongoDB URI (e.g. "mongodb://user:pass@host:27017").
 * @param {string} dbDotCollection    Target as "database.collection" (e.g. "mydb.orders").
 * @returns {DataFrame}
 */
function read_mongodb(connectionString, dbDotCollection) {
    return DataFrame.read_mongodb(connectionString, dbDotCollection);
}

/**
 * Execute a MongoDB command via the Rust native driver.
 *
 * @param {string} connectionString  MongoDB URI.
 * @param {string} database          Target database name.
 * @param {string} commandJson       JSON string of the MongoDB command document.
 *                                   e.g. '{"drop":"my_collection"}'
 * @returns {number}  Value of the 'n' or 'ok' field from the result.
 */
function execute_mongodb(connectionString, database, commandJson) {
    const lib    = getLib();
    const result = lib.pardox_execute_mongodb(connectionString, database, commandJson);
    if (result < 0) {
        throw new Error(`execute_mongodb failed with error code: ${result}`);
    }
    return result;
}

module.exports = {
    read_csv,
    read_sql,
    read_prdx,
    executeSql,
    read_mysql,
    execute_mysql,
    read_sqlserver,
    execute_sqlserver,
    read_mongodb,
    execute_mongodb,
    DEFAULT_CSV_CONFIG,
};
