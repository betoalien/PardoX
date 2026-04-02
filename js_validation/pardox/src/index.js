'use strict';

/**
 * PardoX — Universal High-Performance DataFrame Engine for Node.js
 *
 * Usage:
 *   const { DataFrame, Series, read_csv, read_sql, read_prdx } = require('@pardox/pardox');
 *
 *   // Load from CSV
 *   const df = read_csv('./data.csv');
 *   df.show();
 *
 *   // Column access (Proxy-based subscript)
 *   const col = df['price'];    // → Series
 *   df['tax'] = df['price'].mul(df['rate']);  // column assignment
 *
 *   // Aggregations
 *   console.log(df['price'].sum());
 *   console.log(df['price'].mean());
 *
 *   // Filtering
 *   const mask = df['price'].gt(100);
 *   const filtered = df.filter(mask);
 *
 *   // Write to PostgreSQL
 *   df.toSql('postgresql://user:pass@localhost:5432/db', 'products', 'upsert', ['id']);
 */

const { DataFrame } = require('./DataFrame');
const { Series }    = require('./Series');
const {
    read_csv,
    read_sql,
    read_prdx,
    write_sql_prdx,
    executeSql,
    read_mysql,
    execute_mysql,
    read_sqlserver,
    execute_sqlserver,
    read_mongodb,
    execute_mongodb,
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
    // Gap 30: SQL Cursor API (streaming batch reads)
    queryToResults,
    sqlToParquet,
} = require('./io');
const { getLib } = require('./ffi');

// Gap 17: WebAssembly module
let PardoxWasm = null;
try {
    const path = require('path');
    const fs   = require('fs');
    const wasmPath = path.join(__dirname, '..', 'libs', 'Linux', 'pardox_wasm.js');
    if (fs.existsSync(wasmPath)) {
        PardoxWasm = require(wasmPath).PardoxWasm;
    }
} catch (_) {}

module.exports = {
    // Core classes
    DataFrame,
    Series,

    // IO helpers (functional API, mirrors Python's pardox.read_csv())
    read_csv,
    read_sql,
    read_prdx,
    write_sql_prdx,
    executeSql,
    read_mysql,
    execute_mysql,
    read_sqlserver,
    execute_sqlserver,
    read_mongodb,
    execute_mongodb,

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

    // Gap 30: SQL Cursor API (streaming batch reads)
    queryToResults,
    sqlToParquet,

    // Low-level access (for advanced use)
    getLib,

    // Gap 17: WebAssembly module
    PardoxWasm,
};
