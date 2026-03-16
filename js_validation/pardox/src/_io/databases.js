'use strict';

const { getLib } = require('../ffi');
const { DataFrame } = require('../DataFrame');

/**
 * Read a MySQL table/query into a DataFrame.
 * @param {string} connectionString  MySQL URL (e.g. "mysql://user:pass@host:3306/db").
 * @param {string} query             SQL query.
 * @returns {DataFrame}
 */
function read_mysql(connectionString, query) {
    return DataFrame.read_mysql(connectionString, query);
}

/**
 * Execute DDL/DML against MySQL.
 * @param {string} connectionString
 * @param {string} query
 * @returns {number} Rows affected.
 */
function execute_mysql(connectionString, query) {
    const lib  = getLib();
    const rows = lib.pardox_execute_mysql(connectionString, query);
    if (rows < 0) throw new Error(`execute_mysql failed with error code: ${rows}`);
    return rows;
}

/**
 * Read from SQL Server into a DataFrame.
 * @param {string} connectionString  ADO.NET connection string.
 * @param {string} query             SQL query.
 * @returns {DataFrame}
 */
function read_sqlserver(connectionString, query) {
    return DataFrame.read_sqlserver(connectionString, query);
}

/**
 * Execute DDL/DML against SQL Server.
 * @param {string} connectionString
 * @param {string} query
 * @returns {number} Rows affected.
 */
function execute_sqlserver(connectionString, query) {
    const lib  = getLib();
    const rows = lib.pardox_execute_sqlserver(connectionString, query);
    if (rows < 0) throw new Error(`execute_sqlserver failed with error code: ${rows}`);
    return rows;
}

/**
 * Read a MongoDB collection into a DataFrame.
 * @param {string} connectionString   MongoDB URI.
 * @param {string} dbDotCollection    e.g. "mydb.orders".
 * @returns {DataFrame}
 */
function read_mongodb(connectionString, dbDotCollection) {
    return DataFrame.read_mongodb(connectionString, dbDotCollection);
}

/**
 * Execute a MongoDB command.
 * @param {string} connectionString  MongoDB URI.
 * @param {string} database          Target database name.
 * @param {string} commandJson       JSON string of the command document.
 * @returns {number}
 */
function execute_mongodb(connectionString, database, commandJson) {
    const lib    = getLib();
    const result = lib.pardox_execute_mongodb(connectionString, database, commandJson);
    if (result < 0) throw new Error(`execute_mongodb failed with error code: ${result}`);
    return result;
}

/**
 * Check SQL Server driver configuration.
 * @returns {boolean}
 */
function sqlserverConfigOk() {
    const lib = getLib();
    return Boolean(lib.pardox_sqlserver_config_ok());
}

module.exports = {
    read_mysql,
    execute_mysql,
    read_sqlserver,
    execute_sqlserver,
    read_mongodb,
    execute_mongodb,
    sqlserverConfigOk,
};
