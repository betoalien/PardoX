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

/**
 * Stream a PRDX file directly to PostgreSQL without loading the entire file into memory.
 *
 * This is Gap 14 functionality - streaming RowGroups from PRDX to SQL.
 *
 * @param {string} prdxPath           Path to the .prdx file.
 * @param {string} connectionString   PostgreSQL connection URL.
 * @param {string} tableName          Target table name.
 * @param {string} mode               Write mode - "append" or "replace". Default "append".
 * @param {string[]} conflictCols     Column names for conflict resolution (upsert).
 * @param {number} batchRows         Number of rows per batch. Default 1,000,000.
 * @returns {number}  Number of rows written (positive) or error code (negative).
 */
function write_sql_prdx(prdxPath, connectionString, tableName, mode = 'append', conflictCols = [], batchRows = 1000000) {
    const fs = require('fs');
    if (!fs.existsSync(prdxPath)) {
        throw new Error(`PRDX file not found: ${prdxPath}`);
    }

    const lib = getLib();
    const conflictJson = JSON.stringify(conflictCols);

    const rows = lib.pardox_write_sql_prdx(
        prdxPath,
        connectionString,
        tableName,
        mode,
        conflictJson,
        batchRows
    );

    if (rows < 0) {
        const errorMap = {
            '-1': 'Invalid PRDX path',
            '-2': 'Invalid connection string',
            '-3': 'Invalid table name',
            '-4': 'Invalid mode',
            '-5': 'Invalid conflict columns JSON',
            '-10': 'Failed to open PRDX file',
            '-20': 'PostgreSQL connection failed',
        };
        const msg = errorMap[String(rows)] ?? `Unknown error code: ${rows}`;
        throw new Error(`write_sql_prdx failed: ${msg}`);
    }

    return rows;
}

// =============================================================================
// PARQUET READER
// =============================================================================

/**
 * Load a Parquet file into a PardoX DataFrame.
 * @param {string} path Path to the .parquet file.
 * @returns {DataFrame}
 */
function readParquet(path) {
    const lib = getLib();
    const ptr = lib.pardox_load_manager_parquet(path);
    if (!ptr) throw new Error(`readParquet failed for: ${path}`);
    return new DataFrame(ptr);
}

// =============================================================================
// PRDX FILE STATS (direct file operations)
// =============================================================================

/**
 * Read the schema of a .prdx file without loading data.
 * @param {string} path Path to the .prdx file.
 * @returns {Object} Schema object.
 */
function readPrdxSchema(path) {
    const lib = getLib();
    const result = lib.pardox_read_schema(path);
    return result ? JSON.parse(result) : {};
}

/**
 * Lazily inspect a .prdx file and return metadata.
 * @param {string} path    Path to the .prdx file.
 * @param {Object} options Optional inspection options.
 * @returns {Object} File metadata.
 */
function inspectPrdx(path, options = {}) {
    const lib = getLib();
    const result = lib.pardox_inspect_file_lazy(path, JSON.stringify(options));
    return result ? JSON.parse(result) : {};
}

// =============================================================================
// BULK CSV TO PRDX
// =============================================================================

/**
 * Bulk-convert CSV files matching a glob pattern to a .prdx file.
 * @param {string} srcPattern Glob pattern for source CSV files.
 * @param {string} outPath    Destination .prdx file path.
 * @param {Object} schema     Optional schema override.
 * @param {Object} config     Optional CSV config.
 * @returns {number} Rows copied.
 */
function hyperCopy(srcPattern, outPath, schema = {}, config = {}) {
    const lib = getLib();
    const defaultConfig = { delimiter: 44, quote_char: 34, has_header: true };
    const result = lib.pardox_hyper_copy_v3(
        srcPattern,
        outPath,
        JSON.stringify(Object.keys(schema).length ? schema : {}),
        JSON.stringify(Object.keys(config).length ? config : defaultConfig)
    );
    if (result < 0) throw new Error(`hyperCopy failed with code: ${result}`);
    return result;
}

// =============================================================================
// MAINFRAME IMPORT
// =============================================================================

/**
 * Import a mainframe fixed-width .dat file into a PardoX DataFrame.
 * @param {string} path       Path to the .dat file.
 * @param {Object} schemaJson Schema descriptor.
 * @returns {DataFrame}
 */
function readDat(path, schemaJson = {}) {
    const lib = getLib();
    const ptr = lib.pardox_import_mainframe_dat(path, JSON.stringify(schemaJson));
    if (!ptr) throw new Error(`readDat failed for: ${path}`);
    return new DataFrame(ptr);
}

// =============================================================================
// ENGINE UTILITIES
// =============================================================================

/** Reset the PardoX global engine state. */
function reset() {
    const lib = getLib();
    lib.pardox_reset();
}

/**
 * Ping the PardoX engine.
 * @param {string} systemName System identifier.
 * @returns {number} Response code.
 */
function ping(systemName = 'pardox') {
    const lib = getLib();
    return lib.pardox_legacy_ping(systemName);
}

/**
 * Return the PardoX Core library version string.
 * @returns {string|null}
 */
function engineVersion() {
    const lib = getLib();
    return lib.pardox_version();
}

/**
 * Return a system report from the engine.
 * @returns {Object}
 */
function systemReport() {
    const lib = getLib();
    const result = lib.pardox_get_system_report();
    return result ? JSON.parse(result) : {};
}

/** Return quarantine log entries. */
function getQuarantineLogs() {
    const lib = getLib();
    const result = lib.pardox_get_quarantine_logs();
    return result ? JSON.parse(result) : [];
}

/** Clear all quarantine logs. */
function clearQuarantine() {
    const lib = getLib();
    lib.pardox_clear_quarantine();
}

/** Check SQL Server driver configuration. */
function sqlserverConfigOk() {
    const lib = getLib();
    return Boolean(lib.pardox_sqlserver_config_ok());
}

// =============================================================================
// GAP 20: TIME TRAVEL
// =============================================================================

/**
 * Read a versioned snapshot from disk.
 * @param {string} path  Base directory path.
 * @param {string} label Version label.
 * @returns {DataFrame}
 */
function versionRead(path, label) {
    const lib = getLib();
    const ptr = lib.pardox_version_read(path, label);
    if (!ptr) throw new Error(`versionRead failed: path=${path}, label=${label}`);
    return new DataFrame(ptr);
}

/**
 * List all version labels for a base path.
 * @param {string} path Base directory path.
 * @returns {string[]} Array of version labels.
 */
function versionList(path) {
    const lib = getLib();
    const result = lib.pardox_version_list(path);
    return result ? JSON.parse(result) : [];
}

/**
 * Delete a specific version snapshot.
 * @param {string} path  Base directory path.
 * @param {string} label Version label to delete.
 * @returns {number}
 */
function versionDelete(path, label) {
    const lib = getLib();
    return lib.pardox_version_delete(path, label);
}

// =============================================================================
// GAP 21: ARROW FLIGHT
// =============================================================================

/**
 * Start a local Arrow Flight server.
 * @param {number} port Port number (default 8815).
 * @returns {number}
 */
function flightStart(port = 8815) {
    const lib = getLib();
    return lib.pardox_flight_start(port);
}

/**
 * Register a DataFrame on the Arrow Flight server.
 * @param {string}    name Dataset name.
 * @param {DataFrame} df   DataFrame to register.
 * @returns {number}
 */
function flightRegister(name, df) {
    const lib = getLib();
    return lib.pardox_flight_register(name, df._ptr);
}

/**
 * Read a dataset from an Arrow Flight server.
 * @param {string} server  Server address.
 * @param {number} port    Server port.
 * @param {string} dataset Dataset name.
 * @returns {DataFrame}
 */
function flightRead(server, port, dataset) {
    const lib = getLib();
    const ptr = lib.pardox_flight_read(server, port, dataset);
    if (!ptr) throw new Error(`flightRead failed: ${server}:${port}/${dataset}`);
    return new DataFrame(ptr);
}

/** Stop the local Arrow Flight server. */
function flightStop() {
    const lib = getLib();
    return lib.pardox_flight_stop();
}

// =============================================================================
// GAP 22: CLUSTER / DISTRIBUTED
// =============================================================================

/**
 * Connect to a cluster of PardoX worker nodes.
 * @param {string[]} addresses Worker addresses.
 * @returns {number} Connection handle.
 */
function clusterConnect(addresses) {
    const lib = getLib();
    return lib.pardox_cluster_connect(JSON.stringify(addresses));
}

/** Ping a cluster worker. */
function clusterPing(workerName) {
    const lib = getLib();
    return lib.pardox_cluster_ping(workerName);
}

/** Ping all cluster workers. */
function clusterPingAll() {
    const lib = getLib();
    return lib.pardox_cluster_ping_all();
}

/** Checkpoint cluster state. */
function clusterCheckpoint(path) {
    const lib = getLib();
    return lib.pardox_cluster_checkpoint(path);
}

/** Free the cluster connection. */
function clusterFree() {
    const lib = getLib();
    lib.pardox_cluster_free();
}

/** Start a cluster worker node. */
function clusterWorkerStart(port) {
    const lib = getLib();
    return lib.pardox_cluster_worker_start(port);
}

/** Stop the cluster worker. */
function clusterWorkerStop() {
    const lib = getLib();
    return lib.pardox_cluster_worker_stop();
}

/** Register worker with cluster. */
function clusterWorkerRegister(name, listenAddr) {
    const lib = getLib();
    return lib.pardox_cluster_worker_register(name, listenAddr);
}

/**
 * Scatter a DataFrame across cluster partitions.
 * @param {DataFrame} df         DataFrame to scatter.
 * @param {number}    partitions Number of partitions.
 * @returns {Object|null} Scatter metadata.
 */
function clusterScatter(df, partitions) {
    const lib = getLib();
    const result = lib.pardox_cluster_scatter(df._ptr, partitions);
    return result ? JSON.parse(result) : null;
}

/**
 * Scatter with replication for fault tolerance.
 * @param {DataFrame} df                DataFrame.
 * @param {number}    partitions        Number of partitions.
 * @param {number}    replicationFactor Replication factor.
 * @returns {Object|null}
 */
function clusterScatterResilient(df, partitions, replicationFactor = 2) {
    const lib = getLib();
    const result = lib.pardox_cluster_scatter_resilient(df._ptr, partitions, replicationFactor);
    return result ? JSON.parse(result) : null;
}

/**
 * Execute SQL query across the cluster.
 * @param {string} sql SQL query.
 * @returns {DataFrame}
 */
function clusterSql(sql) {
    const lib = getLib();
    const ptr = lib.pardox_cluster_sql(sql);
    if (!ptr) throw new Error('clusterSql returned null.');
    return new DataFrame(ptr);
}

/**
 * Execute fault-tolerant SQL across the cluster.
 * @param {string} sql SQL query.
 * @returns {DataFrame}
 */
function clusterSqlResilient(sql) {
    const lib = getLib();
    const ptr = lib.pardox_cluster_sql_resilient(sql);
    if (!ptr) throw new Error('clusterSqlResilient returned null.');
    return new DataFrame(ptr);
}

// =============================================================================
// GAP 27: LAZY PIPELINE (extended)
// =============================================================================

/**
 * Create a LazyFrame from a .prdx file (deferred execution).
 * @param {string} path Path to the .prdx file.
 * @returns {*} Opaque LazyFrame pointer.
 */
function scanPrdx(path) {
    const lib = getLib();
    const ptr = lib.pardox_lazy_scan_prdx(path);
    if (!ptr) throw new Error(`scanPrdx failed for: ${path}`);
    return ptr;
}

/**
 * Apply query planner optimizations to a LazyFrame.
 * @param {*} lazyFrame LazyFrame pointer.
 * @returns {*} Optimized LazyFrame pointer.
 */
function lazyOptimize(lazyFrame) {
    const lib = getLib();
    return lib.pardox_lazy_optimize(lazyFrame);
}

/**
 * Return statistics about a lazy plan.
 * @param {*} lazyFrame LazyFrame pointer.
 * @returns {Object|null}
 */
function lazyStats(lazyFrame) {
    const lib = getLib();
    const result = lib.pardox_lazy_stats(lazyFrame);
    return result ? JSON.parse(result) : null;
}

// =============================================================================
// GAP 29: REST CONNECTOR
// =============================================================================

/**
 * Fetch JSON data from a REST API endpoint into a DataFrame.
 * @param {string} url     REST endpoint URL.
 * @param {string} method  HTTP method (default 'GET').
 * @param {Object} headers Optional HTTP headers.
 * @returns {DataFrame}
 */
function readRest(url, method = 'GET', headers = {}) {
    const lib = getLib();
    const ptr = lib.pardox_read_rest(url, method, JSON.stringify(headers));
    if (!ptr) throw new Error(`readRest failed for URL: ${url}`);
    return new DataFrame(ptr);
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
    write_sql_prdx,
    DEFAULT_CSV_CONFIG,
    // Parquet
    readParquet,
    // PRDX metadata
    readPrdxSchema,
    inspectPrdx,
    // Bulk CSV to PRDX
    hyperCopy,
    // Mainframe
    readDat,
    // Engine utilities
    reset,
    ping,
    engineVersion,
    systemReport,
    getQuarantineLogs,
    clearQuarantine,
    sqlserverConfigOk,
    // Gap 20: Time Travel
    versionRead,
    versionList,
    versionDelete,
    // Gap 21: Arrow Flight
    flightStart,
    flightRegister,
    flightRead,
    flightStop,
    // Gap 22: Cluster
    clusterConnect,
    clusterPing,
    clusterPingAll,
    clusterCheckpoint,
    clusterFree,
    clusterWorkerStart,
    clusterWorkerStop,
    clusterWorkerRegister,
    clusterScatter,
    clusterScatterResilient,
    clusterSql,
    clusterSqlResilient,
    // Gap 27: Lazy Pipeline
    scanPrdx,
    lazyOptimize,
    lazyStats,
    // Gap 29: REST Connector
    readRest,
};
