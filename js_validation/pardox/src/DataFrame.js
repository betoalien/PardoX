'use strict';

/**
 * PardoX DataFrame for Node.js
 *
 * Mirrors Python's frame.py and PHP's DataFrame.php.
 *
 * Key design notes (from Node.js integration experience):
 *   - ALL FFI calls are strictly synchronous — no promises/callbacks.
 *   - Types are explicitly declared in ffi.js to avoid calling-convention issues.
 *   - A JavaScript Proxy wraps each DataFrame instance to enable df['col'] subscript
 *     syntax for column selection and column assignment.
 */

const { getLib } = require('./ffi');
const { applyAll, spill } = require('./_ops');

// Default CSV reader configuration (mirrors Python's io.py DEFAULT_CSV_CONFIG)
const DEFAULT_CSV_CONFIG = JSON.stringify({
    delimiter:  44,           // comma
    quote_char: 34,           // double-quote
    has_header: true,
    chunk_size: 16 * 1024 * 1024,
});

// Pipe delimiter CSV config for JSON columns (no quote char to avoid conflicts)
const PIPE_CSV_CONFIG = JSON.stringify({
    delimiter:  124,          // pipe
    quote_char: 0,            // no quote char
    has_header: true,
    chunk_size: 16 * 1024 * 1024,
});

// Internal symbol used to access the raw pointer from outside this module
const PTR_SYMBOL = Symbol('pardox.ptr');

class DataFrame {
    /**
     * @param {*|null} ptr  Opaque koffi pointer to the HyperBlockManager, or null.
     */
    constructor(ptr = null) {
        this[PTR_SYMBOL] = ptr;
        return createProxy(this);
    }

    /**
     * Create a DataFrame directly from an existing Rust pointer.
     * Used internally by all mixin methods that return new DataFrames.
     * @param {*} ptr
     * @returns {DataFrame}
     */
    static _fromPtr(ptr) {
        const df = Object.create(DataFrame.prototype);
        df[PTR_SYMBOL] = ptr;
        return createProxy(df);
    }

    // -------------------------------------------------------------------------
    // Destructor — explicit free required; call free() when done.
    // -------------------------------------------------------------------------

    _free() {
        if (this[PTR_SYMBOL] !== null) {
            const lib = getLib();
            lib.pardox_free_manager(this[PTR_SYMBOL]);
            this[PTR_SYMBOL] = null;
        }
    }

    /** Explicitly free the underlying Rust manager to avoid memory leaks. */
    free() {
        this._free();
    }

    // -------------------------------------------------------------------------
    // Internal accessor — exposes the raw pointer to mixins via this._ptr
    // -------------------------------------------------------------------------

    get _ptr() {
        return this[PTR_SYMBOL];
    }

    set _ptr(value) {
        this[PTR_SYMBOL] = value;
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    _schema() {
        if (!this[PTR_SYMBOL]) return {};
        const lib = getLib();
        const json = lib.pardox_get_schema_json(this[PTR_SYMBOL]);
        if (!json) return {};
        try {
            return JSON.parse(json);
        } catch {
            return {};
        }
    }

    slice(start, length) {
        const lib    = getLib();
        const newPtr = lib.pardox_slice_manager(this[PTR_SYMBOL], start, length);
        if (!newPtr) throw new Error('Slice operation returned null.');
        return DataFrame._fromPtr(newPtr);
    }

    // Internal: assign a Series result as a new column
    _setColumn(colName, series) {
        const { Series } = require('./Series');
        if (!(series instanceof Series)) {
            throw new TypeError('Column assignment requires a PardoX Series.');
        }
        const lib = getLib();
        const res = lib.pardox_add_column(this[PTR_SYMBOL], series._parentPtr(), colName);
        if (res !== 1) {
            const errors = {
                '-1': 'Invalid Pointers',
                '-2': 'Invalid Column Name',
                '-3': 'Engine Logic Error',
            };
            throw new Error(`Column assignment failed: ${errors[String(res)] || `Unknown: ${res}`}`);
        }
    }

    // -------------------------------------------------------------------------
    // Static factory methods
    // -------------------------------------------------------------------------

    /**
     * Load a CSV file using the Rust engine.
     * @param {string}      path     Path to the CSV file.
     * @param {Object|null} schema   Optional: { colName: 'Int64', ... }
     * @param {boolean}     usePipe  Use pipe delimiter (for JSON columns).
     * @returns {DataFrame}
     */
    static read_csv(path, schema = null, usePipe = false) {
        const lib = getLib();
        const fs  = require('fs');
        if (!fs.existsSync(path)) {
            throw new Error(`CSV file not found: ${path}`);
        }
        let schemaJson = '{}';
        if (schema !== null) {
            const entries      = Object.entries(schema);
            const column_names = entries.map(([name]) => name);
            const column_types = entries.map(([, type]) => type);
            schemaJson = JSON.stringify({ column_names, column_types });
        }
        const config = usePipe ? PIPE_CSV_CONFIG : DEFAULT_CSV_CONFIG;
        const ptr    = lib.pardox_load_manager_csv(path, schemaJson, config);
        if (!ptr) throw new Error(`Failed to load CSV: ${path}`);
        return new DataFrame(ptr);
    }

    /**
     * Run a SQL query via the Rust native driver.
     * @param {string} connectionString  PostgreSQL connection URL.
     * @param {string} query             SQL query.
     * @returns {DataFrame}
     */
    static read_sql(connectionString, query) {
        const lib = getLib();
        const ptr = lib.pardox_scan_sql(connectionString, query);
        if (!ptr) throw new Error('SQL query failed. Check connection string and query.');
        return new DataFrame(ptr);
    }

    /**
     * Run a MySQL query via the Rust native driver.
     * @param {string} connectionString  MySQL URL.
     * @param {string} query             SQL query.
     * @returns {DataFrame}
     */
    static read_mysql(connectionString, query) {
        const lib = getLib();
        const ptr = lib.pardox_read_mysql(connectionString, query);
        if (!ptr) throw new Error('MySQL query failed. Check connection string and query.');
        return new DataFrame(ptr);
    }

    /**
     * Run a SQL Server query via the Rust native driver.
     * @param {string} connectionString  ADO.NET connection string.
     * @param {string} query             SQL query.
     * @returns {DataFrame}
     */
    static read_sqlserver(connectionString, query) {
        const lib = getLib();
        const ptr = lib.pardox_read_sqlserver(connectionString, query);
        if (!ptr) throw new Error('SQL Server query failed. Check connection string and query.');
        return new DataFrame(ptr);
    }

    /**
     * Read a MongoDB collection via the Rust native driver.
     * @param {string} connectionString   MongoDB URI.
     * @param {string} dbDotCollection    "database.collection".
     * @returns {DataFrame}
     */
    static read_mongodb(connectionString, dbDotCollection) {
        const lib = getLib();
        const ptr = lib.pardox_read_mongodb(connectionString, dbDotCollection);
        if (!ptr) throw new Error('MongoDB read failed. Check connection string and target.');
        return new DataFrame(ptr);
    }

    /**
     * Create a DataFrame from an array of plain objects.
     * @param {Object[]} data  Array of row objects.
     * @returns {DataFrame}
     */
    static fromArray(data) {
        if (!Array.isArray(data) || data.length === 0) {
            throw new Error('fromArray requires a non-empty array of objects.');
        }
        const lib   = getLib();
        const ndjson = data.map(row => JSON.stringify(row)).join('\n');
        const buf    = Buffer.from(ndjson, 'utf8');
        const ptr    = lib.pardox_read_json_bytes(ndjson, buf.byteLength);
        if (!ptr) throw new Error('Rust engine failed to ingest JSON data.');
        return new DataFrame(ptr);
    }

    /**
     * Load a spilled DataFrame from disk (static factory).
     * @param {string} path
     * @returns {DataFrame}
     */
    static spillFromDisk(path) {
        const lib = getLib();
        const ptr = lib.pardox_spill_from_disk(path);
        if (!ptr) throw new Error('pardox_spill_from_disk returned null.');
        return new DataFrame(ptr);
    }

    /**
     * Get current process memory usage in bytes (static).
     * @returns {number}
     */
    static memoryUsage() {
        const lib = getLib();
        return lib.pardox_memory_usage(null);
    }

    /**
     * Return the number of violations from the last validateContract() call (static).
     * @returns {number}
     */
    static contractViolationCount() {
        const lib = getLib();
        return lib.pardox_contract_violation_count();
    }
}

// Apply all instance method mixins onto the prototype
applyAll(DataFrame.prototype);

// -------------------------------------------------------------------------
// Proxy factory — enables df['col'] and df['col'] = series
// -------------------------------------------------------------------------

// Properties that belong to the DataFrame class itself (not column names).
// Kept in sync with all mixin methods so the Proxy can distinguish them.
const OWN_PROPS = new Set([
    // Core
    '_ptr', '_free', 'free', '_schema', 'slice', '_setColumn',
    'constructor', 'then',   // 'then' prevents Proxy from being treated as a thenable
    // Static factories (referenced by name on prototype chain)
    'read_csv', 'read_sql', 'read_mysql', 'read_sqlserver', 'read_mongodb',
    'fromArray', 'spillFromDisk', 'memoryUsage', 'contractViolationCount', '_fromPtr',
    // metadata (_ops/metadata)
    'shape', 'columns', 'dtypes',
    // visualization (_ops/visualization)
    'show', 'head', 'tail',
    // selection (_ops/selection)
    'iloc', 'filter',
    // mutation (_ops/mutation)
    'cast', 'join', 'fillna', 'round', 'addColumn',
    // writers (_ops/writers)
    'toCsv', 'toPrdx', 'toSql', 'toMysql', 'toSqlserver', 'toMongodb',
    'toParquet', 'writeShardedParquet',
    // export (_ops/export)
    'toJson', 'toDict', 'toList', 'valueCounts', 'unique',
    // math_ops (_ops/math_ops)
    'add', 'sub', 'mul', 'std', 'minMaxScale', 'sortValues',
    // gpu (_ops/gpu)
    'gpuAdd', 'gpuSub', 'gpuMul', 'gpuDiv', 'gpuSqrt', 'gpuExp', 'gpuLog', 'gpuAbs',
    // reshape (_ops/reshape)
    'pivotTable', 'melt',
    // timeseries (_ops/timeseries)
    'ffill', 'bfill', 'interpolate',
    // nested (_ops/nested)
    'jsonExtract', 'explode', 'unnest',
    // spill (_ops/spill)
    'spillToDisk', 'chunkedGroupby', 'externalSort',
    // groupby (_ops/groupby)
    'groupBy',
    // strings (_ops/strings)
    'strUpper', 'strLower', 'strLen', 'strTrim', 'strReplace', 'strContains',
    // datetime (_ops/datetime)
    'dateExtract', 'dateFormat', 'dateAdd', 'dateDiff',
    // decimal (_ops/decimal)
    'decimalFromFloat', 'decimalToFloat', 'decimalRound', 'decimalMulFloat', 'decimalSum',
    // window (_ops/window)
    'windowRolling', 'windowLag', 'windowLead', 'windowRank', 'windowRowNumber',
    // sql (_ops/sql)
    'sql',
    // encryption (_ops/encryption)
    'toPrdxEncrypted',
    // contracts (_ops/contracts)
    'validateContract',
    // timetravel (_ops/timetravel)
    'versionWrite',
    // cluster (_ops/cluster)
    'clusterScatter',
    // linalg (_ops/linalg)
    'linalgL2Normalize', 'linalgL1Normalize', 'linalgCosineSim', 'linalgPca', 'linalgMatmul',
]);

function createProxy(df) {
    return new Proxy(df, {
        get(target, prop, receiver) {
            // Always delegate built-in and class methods/props
            if (
                typeof prop === 'symbol' ||
                OWN_PROPS.has(prop) ||
                prop in DataFrame.prototype ||
                prop.startsWith('_')
            ) {
                return Reflect.get(target, prop, receiver);
            }

            // Treat string keys as column access → return Series
            if (typeof prop === 'string') {
                const { Series } = require('./Series');
                return new Series(target, prop);
            }

            return Reflect.get(target, prop, receiver);
        },

        set(target, prop, value) {
            if (typeof prop === 'string' && !prop.startsWith('_') && !OWN_PROPS.has(prop)) {
                const { Series } = require('./Series');
                if (value instanceof Series) {
                    target._setColumn(prop, value);
                    return true;
                }
            }
            return Reflect.set(target, prop, value);
        },
    });
}

// Export the PTR_SYMBOL so Series can access the raw pointer
module.exports = { DataFrame, PTR_SYMBOL };
