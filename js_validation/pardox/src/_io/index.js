'use strict';

const { read_csv, DEFAULT_CSV_CONFIG } = require('./csv');
const { read_sql, executeSql, write_sql_prdx, queryToResults, sqlToParquet } = require('./sql');
const {
    read_mysql, execute_mysql,
    read_sqlserver, execute_sqlserver,
    read_mongodb, execute_mongodb,
    sqlserverConfigOk,
} = require('./databases');
const {
    read_prdx,
    readPrdxSchema,
    inspectPrdx,
    querySqlPrdx,
    prdxColumnSum,
    prdxMin, prdxMax, prdxMean, prdxCount,
    prdxGroupby,
} = require('./prdx');
const { scanCsv, scanPrdx, lazyOptimize, lazyStats } = require('./lazy');
const { readParquet } = require('./parquet');
const { readCloudCsv, readCloudPrdx, writeCloudPrdx } = require('./cloud');
const { liveQueryStart, liveQueryVersion, liveQueryTake, liveQueryFree } = require('./live_query');
const { readPrdxEncrypted } = require('./encryption');
const { versionRead, versionList, versionDelete } = require('./timetravel');
const { flightStart, flightRegister, flightRead, flightStop } = require('./flight');
const {
    clusterConnect, clusterPing, clusterPingAll,
    clusterCheckpoint, clusterFree,
    clusterWorkerStart, clusterWorkerStop, clusterWorkerRegister,
    clusterScatter, clusterScatterResilient,
    clusterSql, clusterSqlResilient,
} = require('./cluster');
const { readRest } = require('./rest');
const { hyperCopy, readDat, ingestBuffer } = require('./buffers');
const { reset, ping, engineVersion, systemReport, getQuarantineLogs, clearQuarantine } = require('./engine');

module.exports = {
    // CSV
    read_csv,
    DEFAULT_CSV_CONFIG,
    // SQL / PostgreSQL
    read_sql,
    executeSql,
    write_sql_prdx,
    // Gap 30: SQL Cursor API (streaming batch reads)
    queryToResults,
    sqlToParquet,
    // Databases
    read_mysql,
    execute_mysql,
    read_sqlserver,
    execute_sqlserver,
    read_mongodb,
    execute_mongodb,
    sqlserverConfigOk,
    // PRDX
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
    // Lazy / scan
    scanCsv,
    scanPrdx,
    lazyOptimize,
    lazyStats,
    // Parquet
    readParquet,
    // Cloud
    readCloudCsv,
    readCloudPrdx,
    writeCloudPrdx,
    // Live Query
    liveQueryStart,
    liveQueryVersion,
    liveQueryTake,
    liveQueryFree,
    // Encryption
    readPrdxEncrypted,
    // Time Travel
    versionRead,
    versionList,
    versionDelete,
    // Arrow Flight
    flightStart,
    flightRegister,
    flightRead,
    flightStop,
    // Cluster
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
    // REST
    readRest,
    // Buffers / Mainframe / Bulk
    hyperCopy,
    readDat,
    ingestBuffer,
    // Engine utilities
    reset,
    ping,
    engineVersion,
    systemReport,
    getQuarantineLogs,
    clearQuarantine,
};
