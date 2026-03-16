'use strict';

/**
 * PardoX v0.3.2 — JavaScript SDK for napi-rs
 *
 * This module provides a high-level API for the PardoX DataFrame engine
 * using the native Node.js addon (napi-rs).
 */

const path = require('path');
const os = require('os');
const fs = require('fs');

// Determine platform and load appropriate native module
function getNativeModule() {
    const platform = os.platform();
    const arch = os.arch();

    // Map platform/arch to module name
    const platformMap = {
        'linux-x64': 'pardox-napi-Linux-x64.node',
        'darwin-x64': 'pardox-napi-Darwin-x64.node',
        'darwin-arm64': 'pardox-napi-Darwin-ARM64.node',
        'win32-x64': 'pardox_napi_x64.node'
    };

    const key = `${platform}-${arch}`;
    const moduleName = platformMap[key] || 'pardox_napi.node';

    // Try multiple locations
    const searchPaths = [
        path.join(__dirname, '..', 'libs', 'Linux', moduleName),
        path.join(__dirname, '..', 'libs', moduleName),
        path.join(__dirname, '..', 'libs', platform, moduleName),
        path.join(__dirname, '..', 'pardox_napi.node'),
        path.join(__dirname, '..', 'bin', moduleName),
    ];

    // Also try the compiled location
    const compiledPath = path.join(__dirname, '..', '..', '..', 'Pardox-Core-v0.3.2', 'pardox_napi', 'target', 'release', 'libpardox_napi.so');
    searchPaths.push(compiledPath);

    for (const modulePath of searchPaths) {
        if (fs.existsSync(modulePath)) {
            return require(modulePath);
        }
    }

    // Try node_modules style require
    try {
        return require(`@pardox/pardox-napi-${platform}-${arch}`);
    } catch (e) {
        // Fall through
    }

    throw new Error(`PardoX native module not found for ${platform}-${arch}. Searched:\n${searchPaths.join('\n')}`);
}

const native = getNativeModule();

// Initialize the engine
native.init();

// =============================================================================
// DataFrame Class
// =============================================================================

class DataFrame {
    constructor(ndjson) {
        this._ptr = null;
        if (typeof ndjson === 'string') {
            this._ptr = native.fromJson(ndjson);
        } else if (ndjson && typeof ndjson === 'object' && ndjson._ptr) {
            // Clone from another DataFrame
            this._ptr = native.DataFrame.from_ptr ? ndjson._ptr : ndjson._ptr;
        }
        if (!this._ptr) {
            throw new Error('Failed to create DataFrame');
        }
    }

    static fromPtr(ptr) {
        const df = Object.create(DataFrame.prototype);
        df._ptr = ptr;
        return df;
    }

    // -------------------------------------------------------------------------
    // Static factory methods
    // -------------------------------------------------------------------------

    static readCsv(path, schema = null) {
        const df = Object.create(DataFrame.prototype);
        df._ptr = native.readCsv(path, schema);
        if (!df._ptr) throw new Error('Failed to read CSV');
        return df;
    }

    static readPrdx(path, limit = null) {
        const df = Object.create(DataFrame.prototype);
        df._ptr = native.readPrdx(path, limit);
        if (!df._ptr) throw new Error('Failed to read PRDX');
        return df;
    }

    static readParquet(path) {
        const df = Object.create(DataFrame.prototype);
        df._ptr = native.readParquet(path);
        if (!df._ptr) throw new Error('Failed to read Parquet');
        return df;
    }

    static fromJson(jsonStr) {
        const df = Object.create(DataFrame.prototype);
        df._ptr = native.fromJson(jsonStr);
        if (!df._ptr) throw new Error('Failed to parse JSON');
        return df;
    }

    // -------------------------------------------------------------------------
    // Properties
    // -------------------------------------------------------------------------

    get rowCount() {
        return this._ptr.rowCount;
    }

    get shape() {
        return [this._ptr.rowCount, this._ptr.columnNames().length];
    }

    get columns() {
        return this._ptr.columnNames();
    }

    // -------------------------------------------------------------------------
    // Basic operations
    // -------------------------------------------------------------------------

    schema() {
        return JSON.parse(this._ptr.schemaJson());
    }

    head(n = 5) {
        const ptr = this._ptr.head(n);
        return DataFrame.fromPtr(ptr);
    }

    tail(n = 5) {
        const ptr = this._ptr.tail(n);
        return DataFrame.fromPtr(ptr);
    }

    iloc(start, len) {
        const ptr = this._ptr.iloc(start, len);
        return DataFrame.fromPtr(ptr);
    }

    slice(start, len) {
        return this.iloc(start, len);
    }

    toCsv(path) {
        return this._ptr.toCsv(path);
    }

    toPrdx(path) {
        return this._ptr.toPrdx(path);
    }

    toJson(n = null) {
        return this._ptr.toJson(n || this.rowCount);
    }

    show(n = 10) {
        console.log(this._ptr.toAscii(n));
    }

    // -------------------------------------------------------------------------
    // Aggregations
    // -------------------------------------------------------------------------

    sum(col) {
        return this._ptr.sum(col);
    }

    mean(col) {
        return this._ptr.mean(col);
    }

    min(col) {
        return this._ptr.min(col);
    }

    max(col) {
        return this._ptr.max(col);
    }

    count(col) {
        return this._ptr.count(col);
    }

    std(col) {
        return this._ptr.std(col);
    }

    // -------------------------------------------------------------------------
    // Filtering
    // -------------------------------------------------------------------------

    filter(maskDf, maskCol) {
        const ptr = this._ptr.filter(maskDf._ptr, maskCol);
        return DataFrame.fromPtr(ptr);
    }

    gt(col, value) {
        const ptr = this._ptr.gt(col, value);
        return DataFrame.fromPtr(ptr);
    }

    lt(col, value) {
        const ptr = this._ptr.lt(col, value);
        return DataFrame.fromPtr(ptr);
    }

    eq(col, value) {
        const ptr = this._ptr.eq(col, value);
        return DataFrame.fromPtr(ptr);
    }

    // -------------------------------------------------------------------------
    // Sorting
    // -------------------------------------------------------------------------

    sortValues(col, descending = false) {
        const ptr = this._ptr.sortValues(col, descending);
        return DataFrame.fromPtr(ptr);
    }

    // -------------------------------------------------------------------------
    // Column operations
    // -------------------------------------------------------------------------

    colAdd(leftCol, rightCol) {
        const ptr = this._ptr.colAdd(leftCol, rightCol);
        return DataFrame.fromPtr(ptr);
    }

    colSub(leftCol, rightCol) {
        const ptr = this._ptr.colSub(leftCol, rightCol);
        return DataFrame.fromPtr(ptr);
    }

    colMul(leftCol, rightCol) {
        const ptr = this._ptr.colMul(leftCol, rightCol);
        return DataFrame.fromPtr(ptr);
    }

    colDiv(leftCol, rightCol) {
        const ptr = this._ptr.colDiv(leftCol, rightCol);
        return DataFrame.fromPtr(ptr);
    }

    colGt(leftCol, rightCol) {
        const ptr = this._ptr.colGt(leftCol, rightCol);
        return DataFrame.fromPtr(ptr);
    }

    colLt(leftCol, rightCol) {
        const ptr = this._ptr.colLt(leftCol, rightCol);
        return DataFrame.fromPtr(ptr);
    }

    colEq(leftCol, rightCol) {
        const ptr = this._ptr.colEq(leftCol, rightCol);
        return DataFrame.fromPtr(ptr);
    }

    // -------------------------------------------------------------------------
    // Type conversion and data cleaning
    // -------------------------------------------------------------------------

    cast(colName, targetType) {
        return this._ptr.cast(colName, targetType);
    }

    fillna(colName, value) {
        return this._ptr.fillna(colName, value);
    }

    round(colName, decimals) {
        return this._ptr.round(colName, decimals);
    }

    addColumn(sourceDf, colName) {
        return this._ptr.addColumn(sourceDf._ptr, colName);
    }

    // -------------------------------------------------------------------------
    // Join
    // -------------------------------------------------------------------------

    join(rightDf, leftKey, rightKey) {
        const ptr = this._ptr.join(rightDf._ptr, leftKey, rightKey);
        return DataFrame.fromPtr(ptr);
    }

    // -------------------------------------------------------------------------
    // GroupBy (Gap 1)
    // -------------------------------------------------------------------------

    groupBy(groupCols, aggSpec) {
        const cols = Array.isArray(groupCols) ? groupCols : [groupCols];
        const ptr = native.groupbyAgg(this._ptr, cols, Object.keys(aggSpec)[0], Object.values(aggSpec)[0]);
        return DataFrame.fromPtr(ptr);
    }

    groupByMulti(groupCols, aggSpec) {
        // For multiple aggregations, we need to call groupbyAgg with JSON
        const cols = Array.isArray(groupCols) ? groupCols : [groupCols];
        // This is a simplified version - for complex aggregations use sqlQuery
        const firstCol = Object.keys(aggSpec)[0];
        const firstAgg = aggSpec[firstCol];
        const ptr = native.groupbyAgg(this._ptr, cols, firstCol, firstAgg);
        return DataFrame.fromPtr(ptr);
    }

    // -------------------------------------------------------------------------
    // SQL Query (Gap 14)
    // -------------------------------------------------------------------------

    sqlQuery(sql) {
        const ptr = native.sqlQuery(this._ptr, sql);
        return DataFrame.fromPtr(ptr);
    }

    // -------------------------------------------------------------------------
    // Memory management
    // -------------------------------------------------------------------------

    free() {
        if (this._ptr) {
            this._ptr.free();
            this._ptr = null;
        }
    }

    _free() {
        this.free();
    }
}

// =============================================================================
// Module exports
// =============================================================================

module.exports = {
    DataFrame,
    init: () => native.init(),
    getSystemReport: () => native.getSystemReport(),

    // Static read functions
    readCsv: (path, schema) => DataFrame.readCsv(path, schema),
    readPrdx: (path, limit) => DataFrame.readPrdx(path, limit),
    readParquet: (path) => DataFrame.readParquet(path),
    fromJson: (jsonStr) => DataFrame.fromJson(jsonStr),

    // PRDX direct functions (Gap 12-13)
    readHeadJson: (path, n) => native.readHeadJson(path, n),
    columnSum: (path, col) => native.columnSum(path, col),
    prdxMin: (path, col) => native.prdxMin(path, col),
    prdxMax: (path, col) => native.prdxMax(path, col),
    prdxMean: (path, col) => native.prdxMean(path, col),
    prdxCount: (path) => native.prdxCount(path),
    queryPrdx: (path, sql) => native.queryPrdx(path, sql),

    // Hyper Copy
    hyperCopyV3: (pattern, outPath, schemaJson, configJson) =>
        native.hyperCopyV3(pattern, outPath, schemaJson, configJson),

    // Gap 2: String functions
    strUpper: (df, col) => { const ptr = native.strUpper(df._ptr, col); return DataFrame.fromPtr(ptr); },
    strLower: (df, col) => { const ptr = native.strLower(df._ptr, col); return DataFrame.fromPtr(ptr); },
    strContains: (df, col, pattern) => { const ptr = native.strContains(df._ptr, col, pattern); return DataFrame.fromPtr(ptr); },
    strReplace: (df, col, from, to) => { const ptr = native.strReplace(df._ptr, col, from, to); return DataFrame.fromPtr(ptr); },
    strTrim: (df, col) => { const ptr = native.strTrim(df._ptr, col); return DataFrame.fromPtr(ptr); },
    strLen: (df, col) => { const ptr = native.strLen(df._ptr, col); return DataFrame.fromPtr(ptr); },

    // Gap 2: Date functions
    dateExtract: (df, col, component) => { const ptr = native.dateExtract(df._ptr, col, component); return DataFrame.fromPtr(ptr); },
    dateFormat: (df, col, format) => { const ptr = native.dateFormat(df._ptr, col, format); return DataFrame.fromPtr(ptr); },
    dateDiff: (df, colA, colB, unit) => { const ptr = native.dateDiff(df._ptr, colA, colB, unit); return DataFrame.fromPtr(ptr); },
    dateAdd: (df, col, days) => { const ptr = native.dateAdd(df._ptr, col, days); return DataFrame.fromPtr(ptr); },

    // Gap 3: Decimal
    decimalFromFloat: (df, col, scale) => { const ptr = native.decimalFromFloat(df._ptr, col, scale); return DataFrame.fromPtr(ptr); },
    decimalSum: (df, col) => native.decimalSum(df._ptr, col),
    decimalGetScale: (df, col) => native.decimalGetScale(df._ptr, col),

    // Gap 4: Window functions
    rowNumber: (df, orderCol, asc = true) => { const ptr = native.rowNumber(df._ptr, orderCol, asc); return DataFrame.fromPtr(ptr); },
    rank: (df, orderCol, asc = true) => { const ptr = native.rank(df._ptr, orderCol, asc); return DataFrame.fromPtr(ptr); },
    lag: (df, col, offset, defaultVal = 0) => { const ptr = native.lag(df._ptr, col, offset, defaultVal); return DataFrame.fromPtr(ptr); },
    lead: (df, col, offset, defaultVal = 0) => { const ptr = native.lead(df._ptr, col, offset, defaultVal); return DataFrame.fromPtr(ptr); },
    rollingMean: (df, col, window) => { const ptr = native.rollingMean(df._ptr, col, window); return DataFrame.fromPtr(ptr); },

    // Gap 7: GPU Sort
    gpuSort: (df, col) => { const ptr = native.gpuSort(df._ptr, col); return DataFrame.fromPtr(ptr); },

    // Gap 7: GPU Arithmetic (binary operations)
    gpuAdd: (df, colA, colB) => { const ptr = native.gpuAdd(df._ptr, colA, colB); return DataFrame.fromPtr(ptr); },
    gpuSub: (df, colA, colB) => { const ptr = native.gpuSub(df._ptr, colA, colB); return DataFrame.fromPtr(ptr); },
    gpuMul: (df, colA, colB) => { const ptr = native.gpuMul(df._ptr, colA, colB); return DataFrame.fromPtr(ptr); },
    gpuDiv: (df, colA, colB) => { const ptr = native.gpuDiv(df._ptr, colA, colB); return DataFrame.fromPtr(ptr); },

    // Gap 7: GPU Arithmetic (unary operations)
    gpuSqrt: (df, col) => { const ptr = native.gpuSqrt(df._ptr, col); return DataFrame.fromPtr(ptr); },
    gpuExp: (df, col) => { const ptr = native.gpuExp(df._ptr, col); return DataFrame.fromPtr(ptr); },
    gpuLog: (df, col) => { const ptr = native.gpuLog(df._ptr, col); return DataFrame.fromPtr(ptr); },
    gpuAbs: (df, col) => { const ptr = native.gpuAbs(df._ptr, col); return DataFrame.fromPtr(ptr); },

    // Gap 8: Pivot & Melt
    pivot: (df, index, columns, values, aggFn) => {
        const ptr = native.pivot(df._ptr, index, columns, values, aggFn);
        return DataFrame.fromPtr(ptr);
    },
    melt: (df, idVars, valueVars, varName, valName) => {
        const ptr = native.melt(df._ptr, idVars, valueVars, varName, valName);
        return DataFrame.fromPtr(ptr);
    },

    // Gap 9: Time Series Fill
    ffill: (df, col) => { const ptr = native.ffill(df._ptr, col); return DataFrame.fromPtr(ptr); },
    bfill: (df, col) => { const ptr = native.bfill(df._ptr, col); return DataFrame.fromPtr(ptr); },
    interpolate: (df, col) => { const ptr = native.interpolate(df._ptr, col); return DataFrame.fromPtr(ptr); },

    // Gap 10: Nested Data
    jsonExtract: (df, col, keyPath) => { const ptr = native.jsonExtract(df._ptr, col, keyPath); return DataFrame.fromPtr(ptr); },
    explode: (df, col) => { const ptr = native.explode(df._ptr, col); return DataFrame.fromPtr(ptr); },
    unnest: (df, col) => { const ptr = native.unnest(df._ptr, col); return DataFrame.fromPtr(ptr); },

    // Gap 11: Spill to Disk
    spillToDisk: (df, path) => native.spillToDisk(df._ptr, path),
    spillFromDisk: (path) => { const ptr = native.spillFromDisk(path); return DataFrame.fromPtr(ptr); },
    memoryUsage: () => native.memoryUsage(),

    // Gap 15: Cloud Storage
    cloudReadCsv: (url, schema, config, creds) => {
        const ptr = native.cloudReadCsv(url, schema, config, creds);
        return DataFrame.fromPtr(ptr);
    },
    cloudReadPrdx: (url, creds) => {
        const ptr = native.cloudReadPrdx(url, creds);
        return DataFrame.fromPtr(ptr);
    },
    cloudWritePrdx: (df, url, creds) => native.cloudWritePrdx(df._ptr, url, creds),

    // Gap 18: Encryption
    writePrdxEncrypted: (df, path, cols, password) => native.writePrdxEncrypted(df._ptr, path, cols, password),
    readPrdxEncrypted: (path, password) => {
        const ptr = native.readPrdxEncrypted(path, password);
        return DataFrame.fromPtr(ptr);
    },

    // Gap 19: Data Contracts
    validateContract: (df, contractJson) => {
        const ptr = native.validateContract(df._ptr, contractJson);
        return DataFrame.fromPtr(ptr);
    },
    contractViolationCount: () => native.contractViolationCount(),
    getQuarantineLogs: () => native.getQuarantineLogs(),
    clearQuarantine: () => native.clearQuarantine(),

    // Gap 20: Time Travel
    versionWrite: (df, store, tag, maxVersions) => native.versionWrite(df._ptr, store, tag, maxVersions),
    versionRead: (store, tag) => {
        const ptr = native.versionRead(store, tag);
        return DataFrame.fromPtr(ptr);
    },
    versionList: (store) => native.versionList(store),
    versionDelete: (store, tag) => native.versionDelete(store, tag),

    // Gap 21: Arrow Flight
    flightStart: (port) => native.flightStart(port),
    flightRegister: (df, name) => native.flightRegister(df._ptr, name),
    flightStop: () => native.flightStop(),
    flightRead: (host, port, name) => {
        const ptr = native.flightRead(host, port, name);
        return DataFrame.fromPtr(ptr);
    },

    // Gap 22: Distributed Cluster
    clusterWorkerStart: (port) => native.clusterWorkerStart(port),
    clusterWorkerStop: () => native.clusterWorkerStop(),
    clusterWorkerRegister: (name, df) => native.clusterWorkerRegister(name, df._ptr),

    // Gap 23: SQL Server Config
    sqlserverConfigOk: (connStr) => native.sqlserverConfigOk(connStr),

    // Gap 28: Linear Algebra
    matmul: (a, b) => { const ptr = native.matmul(a._ptr, b._ptr); return DataFrame.fromPtr(ptr); },
    l2Normalize: (df) => { const ptr = native.l2Normalize(df._ptr); return DataFrame.fromPtr(ptr); },
    l1Normalize: (df) => { const ptr = native.l1Normalize(df._ptr); return DataFrame.fromPtr(ptr); },
    cosineSim: (a, b) => native.cosineSim(a._ptr, b._ptr),
    pca: (df, nComponents) => { const ptr = native.pca(df._ptr, nComponents); return DataFrame.fromPtr(ptr); },

    // Gap 29: REST Connector
    readRest: (url, config) => {
        const ptr = native.readRest(url, config);
        return DataFrame.fromPtr(ptr);
    },

    // Database Connectors
    readSql: (connStr, query) => {
        const ptr = native.readSql(connStr, query);
        return DataFrame.fromPtr(ptr);
    },
    readMysql: (connStr, query) => {
        const ptr = native.readMysql(connStr, query);
        return DataFrame.fromPtr(ptr);
    },
    readSqlServer: (connStr, query) => {
        const ptr = native.readSqlServer(connStr, query);
        return DataFrame.fromPtr(ptr);
    },
    readMongoDb: (connStr, database, collection) => {
        const ptr = native.readMongoDb(connStr, database, collection);
        return DataFrame.fromPtr(ptr);
    },

    // Column comparison (for filters)
    colGt: (df, leftCol, rightCol) => { const ptr = native.colGt(df._ptr, leftCol, rightCol); return DataFrame.fromPtr(ptr); },
    colLt: (df, leftCol, rightCol) => { const ptr = native.colLt(df._ptr, leftCol, rightCol); return DataFrame.fromPtr(ptr); },
    colEq: (df, leftCol, rightCol) => { const ptr = native.colEq(df._ptr, leftCol, rightCol); return DataFrame.fromPtr(ptr); },

    // DataFrame utilities
    valueCounts: (df, colName) => { const ptr = native.valueCounts(df._ptr, colName); return DataFrame.fromPtr(ptr); },
    unique: (df, colName) => { const ptr = native.unique(df._ptr, colName); return DataFrame.fromPtr(ptr); },
    toJsonRecords: (df) => native.toJsonRecords(df._ptr),
    toJsonArrays: (df) => native.toJsonArrays(df._ptr),

    // External operations
    externalSort: (df, colName, ascending, chunkSize) => { const ptr = native.externalSort(df._ptr, colName, ascending, chunkSize); return DataFrame.fromPtr(ptr); },
    chunkedGroupby: (df, groupCols, aggJson, chunkSize) => { const ptr = native.chunkedGroupby(df._ptr, groupCols, aggJson, chunkSize); return DataFrame.fromPtr(ptr); },

    // Hash join
    hashJoin: (leftDf, rightDf, leftKey, rightKey) => { const ptr = native.hashJoin(leftDf._ptr, rightDf._ptr, leftKey, rightKey); return DataFrame.fromPtr(ptr); },

    // Gap 5/27: Lazy Pipeline
    lazyScanCsv: (path, schema, config) => native.lazyScanCsv(path, schema, config),
    lazyScanPrdx: (path) => native.lazyScanPrdx(path),
    lazyOptimize: (lf) => native.lazyOptimize(lf),
    lazyStats: (lf) => native.lazyStats(lf),

    // Gap 22: Cluster (extended)
    clusterConnect: (addrsJson) => native.clusterConnect(addrsJson),
    clusterPing: (conn) => native.clusterPing(conn),
    clusterPingAll: (conn) => native.clusterPingAll(conn),
    clusterCheckpoint: (conn, path) => native.clusterCheckpoint(conn, path),
    clusterFree: (conn) => native.clusterFree(conn),
    clusterScatter: (conn, name, df) => native.clusterScatter(conn, name, df._ptr),
    clusterScatterResilient: (conn, name, df) => native.clusterScatterResilient(conn, name, df._ptr),
    clusterSql: (conn, name, sql) => {
        const ptr = native.clusterSql(conn, name, sql);
        return DataFrame.fromPtr(ptr);
    },
    clusterSqlResilient: (conn, name, sql) => {
        const ptr = native.clusterSqlResilient(conn, name, sql);
        return DataFrame.fromPtr(ptr);
    },

    // Mainframe
    readDat: (srcPath, outPath, schemaJson) => native.readDat(srcPath, outPath, schemaJson),

    // PRDX metadata (no full load)
    inspectPrdx: (path, optionsJson) => native.inspectPrdx(path, optionsJson || '{}'),
    readPrdxSchema: (path) => native.readPrdxSchema(path),

    // Engine utilities
    engineVersion: () => native.engineVersion(),
    reset: () => native.reset(),
    ping: () => native.ping(),

    // DataFrame Parquet write methods
    toParquet: (df, path) => native.toParquet(df._ptr, path),
    writeShardedParquet: (df, dir, maxRows) => native.writeShardedParquet(df._ptr, dir, maxRows),

    // Raw buffer ingestion
    ingestBuffer: (bufPtr, bufLen, layoutJson, schemaJson) =>
        native.ingestBuffer(bufPtr, bufLen, layoutJson, schemaJson),

    // Low-level access
    getNative: () => native,
};