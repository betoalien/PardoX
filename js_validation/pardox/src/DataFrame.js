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

// Default CSV reader configuration (mirrors Python's io.py DEFAULT_CSV_CONFIG)
const DEFAULT_CSV_CONFIG = JSON.stringify({
    delimiter:  44,           // comma
    quote_char: 34,           // double-quote
    has_header: true,
    chunk_size: 16 * 1024 * 1024,
});

// Internal symbol used to access the raw pointer from outside this module
const PTR_SYMBOL = Symbol('pardox.ptr');

class DataFrame {
    /**
     * @param {*|null}  ptr      Opaque koffi pointer to the HyperBlockManager, or null.
     */
    constructor(ptr = null) {
        this[PTR_SYMBOL] = ptr;
        return createProxy(this);
    }

    // -------------------------------------------------------------------------
    // Destructor — Node.js FinalizationRegistry handles GC-based freeing
    // -------------------------------------------------------------------------

    _free() {
        if (this[PTR_SYMBOL] !== null) {
            const lib = getLib();
            lib.pardox_free_manager(this[PTR_SYMBOL]);
            this[PTR_SYMBOL] = null;
        }
    }

    // -------------------------------------------------------------------------
    // Properties
    // -------------------------------------------------------------------------

    get shape() {
        const lib = getLib();
        const rows = lib.pardox_get_row_count(this[PTR_SYMBOL]);
        const cols = this.columns.length;
        return [rows, cols];
    }

    get columns() {
        const schema = this._schema();
        return (schema.columns || []).map(c => c.name);
    }

    get dtypes() {
        const schema = this._schema();
        const result = {};
        for (const col of (schema.columns || [])) {
            result[col.name] = col.type;
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Factory Methods
    // -------------------------------------------------------------------------

    /**
     * Load a CSV file using the Rust engine.
     * @param {string}      path    Path to the CSV file.
     * @param {Object|null} schema  Optional: { colName: 'Int64', ... }
     * @returns {DataFrame}
     */
    static read_csv(path, schema = null) {
        const lib = getLib();
        const fs = require('fs');

        if (!fs.existsSync(path)) {
            throw new Error(`CSV file not found: ${path}`);
        }

        let schemaJson = '{}';
        if (schema !== null) {
            const cols = Object.entries(schema).map(([name, type]) => ({ name, type }));
            schemaJson = JSON.stringify({ columns: cols });
        }

        const ptr = lib.pardox_load_manager_csv(path, schemaJson, DEFAULT_CSV_CONFIG);

        if (!ptr) {
            throw new Error(`Failed to load CSV: ${path}`);
        }

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

        if (!ptr) {
            throw new Error('SQL query failed. Check connection string and query.');
        }

        return new DataFrame(ptr);
    }

    /**
     * Run a MySQL query via the Rust native driver.
     * @param {string} connectionString  MySQL URL (e.g. "mysql://user:pass@host:3306/db").
     * @param {string} query             SQL query.
     * @returns {DataFrame}
     */
    static read_mysql(connectionString, query) {
        const lib = getLib();
        const ptr = lib.pardox_read_mysql(connectionString, query);
        if (!ptr) {
            throw new Error('MySQL query failed. Check connection string and query.');
        }
        return new DataFrame(ptr);
    }

    /**
     * Run a SQL Server query via the Rust native driver (tiberius).
     * @param {string} connectionString  ADO.NET connection string.
     * @param {string} query             SQL query.
     * @returns {DataFrame}
     */
    static read_sqlserver(connectionString, query) {
        const lib = getLib();
        const ptr = lib.pardox_read_sqlserver(connectionString, query);
        if (!ptr) {
            throw new Error('SQL Server query failed. Check connection string and query.');
        }
        return new DataFrame(ptr);
    }

    /**
     * Read a MongoDB collection via the Rust native driver.
     * @param {string} connectionString   MongoDB URI (e.g. "mongodb://user:pass@host:27017").
     * @param {string} dbDotCollection    Target as "database.collection".
     * @returns {DataFrame}
     */
    static read_mongodb(connectionString, dbDotCollection) {
        const lib = getLib();
        const ptr = lib.pardox_read_mongodb(connectionString, dbDotCollection);
        if (!ptr) {
            throw new Error('MongoDB read failed. Check connection string and target.');
        }
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

        const lib = getLib();
        const ndjson = data.map(row => JSON.stringify(row)).join('\n');
        const buf = Buffer.from(ndjson, 'utf8');

        const ptr = lib.pardox_read_json_bytes(ndjson, buf.byteLength);

        if (!ptr) {
            throw new Error('Rust engine failed to ingest JSON data.');
        }

        return new DataFrame(ptr);
    }

    // -------------------------------------------------------------------------
    // Inspection & Visualization
    // -------------------------------------------------------------------------

    /** Return a new DataFrame with the first N rows. */
    head(n = 5) {
        return this.slice(0, n);
    }

    /** Return a new DataFrame with the last N rows. */
    tail(n = 5) {
        const lib = getLib();
        const newPtr = lib.pardox_tail_manager(this[PTR_SYMBOL], n);
        if (!newPtr) throw new Error('tail() failed (Rust returned null).');
        return new DataFrame(newPtr);
    }

    /** Print N rows as an ASCII table to stdout. */
    show(n = 10) {
        const lib = getLib();
        const ascii = lib.pardox_manager_to_ascii(this[PTR_SYMBOL], n);
        if (ascii) console.log('\n' + ascii);
    }

    /** Serialize up to `limit` rows as a JSON string. */
    toJson(limit = 100) {
        const lib = getLib();
        return lib.pardox_manager_to_json(this[PTR_SYMBOL], limit) || '[]';
    }

    /** Return a slice (new DataFrame) by row positions [start, end). */
    iloc(start, end) {
        return this.slice(start, end - start);
    }

    // -------------------------------------------------------------------------
    // Mutation & Transformation
    // -------------------------------------------------------------------------

    /** Cast a column to a new type in-place. Returns `this` for chaining. */
    cast(col, targetType) {
        const lib = getLib();
        const res = lib.pardox_cast_column(this[PTR_SYMBOL], col, targetType);
        if (res !== 1) {
            throw new Error(`Failed to cast column '${col}' to '${targetType}'.`);
        }
        return this;
    }

    /**
     * Hash-join with another DataFrame.
     * @param {DataFrame}   other
     * @param {string|null} on        Key column (same name on both sides).
     * @param {string|null} leftOn    Left key column.
     * @param {string|null} rightOn   Right key column.
     */
    join(other, { on = null, leftOn = null, rightOn = null } = {}) {
        const lCol = on || leftOn;
        const rCol = on || rightOn;

        if (!lCol || !rCol) {
            throw new Error("Specify 'on' or both 'leftOn' and 'rightOn'.");
        }

        const lib = getLib();
        const resPtr = lib.pardox_hash_join(this[PTR_SYMBOL], other[PTR_SYMBOL], lCol, rCol);

        if (!resPtr) throw new Error('Join failed (Rust returned null).');
        return new DataFrame(resPtr);
    }

    /**
     * Apply a boolean Series as a row filter.
     * @param {Series} mask  Boolean Series produced by a comparison operation.
     */
    filter(mask) {
        const { Series } = require('./Series');

        if (!(mask instanceof Series)) {
            throw new TypeError('filter() requires a PardoX Series mask.');
        }

        const lib = getLib();
        const resPtr = lib.pardox_apply_filter(
            this[PTR_SYMBOL],
            mask._parentPtr(),
            mask.name
        );

        if (!resPtr) throw new Error('Filter operation returned null.');
        return new DataFrame(resPtr);
    }

    /** Fill null/NaN in all numeric columns. Returns `this` for chaining. */
    fillna(val) {
        const lib = getLib();
        for (const [col, dtype] of Object.entries(this.dtypes)) {
            if (['Float64', 'Int64'].includes(dtype)) {
                lib.pardox_fill_na(this[PTR_SYMBOL], col, val);
            }
        }
        return this;
    }

    /** Round all numeric columns to N decimal places. Returns `this` for chaining. */
    round(decimals = 0) {
        const lib = getLib();
        for (const col of this.columns) {
            lib.pardox_round(this[PTR_SYMBOL], col, decimals);
        }
        return this;
    }

    // -------------------------------------------------------------------------
    // Writers
    // -------------------------------------------------------------------------

    /** Export to CSV. */
    toCsv(path) {
        const lib = getLib();
        const res = lib.pardox_to_csv(this[PTR_SYMBOL], path);
        if (res !== 1) {
            const errors = {
                '-1': 'Invalid Manager Pointer',
                '-2': 'Invalid Path String',
                '-3': 'Failed to initialize CSV Writer',
                '-4': 'Failed to write header',
                '-5': 'Failed to write data block',
                '-6': 'Failed to flush buffer to disk',
            };
            throw new Error(`toCsv failed: ${errors[String(res)] || `Unknown code: ${res}`}`);
        }
        return true;
    }

    /** Export to native PardoX binary format (.prdx). */
    toPrdx(path) {
        const lib = getLib();
        const res = lib.pardox_to_prdx(this[PTR_SYMBOL], path);
        if (res !== 1) throw new Error(`toPrdx failed with code: ${res}`);
        return true;
    }

    /**
     * Write to a PostgreSQL table.
     * @param {string}   connectionString  PostgreSQL connection URL.
     * @param {string}   tableName         Target table name.
     * @param {string}   mode              'append' (default) or 'upsert'.
     * @param {string[]} conflictCols      Columns for ON CONFLICT clause.
     * @returns {number} Rows written.
     */
    toSql(connectionString, tableName, mode = 'append', conflictCols = []) {
        if (!['append', 'upsert'].includes(mode)) {
            throw new Error("mode must be 'append' or 'upsert'.");
        }

        const lib = getLib();
        const colsJson = JSON.stringify(conflictCols);

        const rows = lib.pardox_write_sql(
            this[PTR_SYMBOL],
            connectionString,
            tableName,
            mode,
            colsJson
        );

        if (rows < 0) {
            const errors = {
                '-1':   'Invalid Manager Pointer',
                '-2':   'Invalid connection string',
                '-3':   'Invalid table name',
                '-4':   'Invalid mode string',
                '-5':   'Invalid conflict_cols JSON',
                '-100': 'Write operation failed (check connection and table schema)',
            };
            throw new Error(`toSql failed: ${errors[String(rows)] || `Unknown code: ${rows}`}`);
        }

        return rows;
    }

    /**
     * Write to a MySQL table.
     * @param {string}   connectionString  MySQL URL (e.g. "mysql://user:pass@host:3306/db").
     * @param {string}   tableName         Target table name.
     * @param {string}   mode              'append' (default), 'replace', or 'upsert'.
     * @param {string[]} conflictCols      Columns for ON DUPLICATE KEY UPDATE clause.
     * @returns {number} Rows written.
     */
    toMysql(connectionString, tableName, mode = 'append', conflictCols = []) {
        if (!['append', 'replace', 'upsert'].includes(mode)) {
            throw new Error("mode must be 'append', 'replace', or 'upsert'.");
        }
        const lib  = getLib();
        const rows = lib.pardox_write_mysql(
            this[PTR_SYMBOL],
            connectionString,
            tableName,
            mode,
            JSON.stringify(conflictCols)
        );
        if (rows < 0) {
            throw new Error(`toMysql failed with error code: ${rows}`);
        }
        return rows;
    }

    /**
     * Write to a SQL Server table.
     * @param {string}   connectionString  ADO.NET connection string.
     * @param {string}   tableName         Target table name.
     * @param {string}   mode              'append' (default), 'replace', or 'upsert'.
     * @param {string[]} conflictCols      Columns for MERGE ON clause.
     * @returns {number} Rows written.
     */
    toSqlserver(connectionString, tableName, mode = 'append', conflictCols = []) {
        if (!['append', 'replace', 'upsert'].includes(mode)) {
            throw new Error("mode must be 'append', 'replace', or 'upsert'.");
        }
        const lib  = getLib();
        const rows = lib.pardox_write_sqlserver(
            this[PTR_SYMBOL],
            connectionString,
            tableName,
            mode,
            JSON.stringify(conflictCols)
        );
        if (rows < 0) {
            throw new Error(`toSqlserver failed with error code: ${rows}`);
        }
        return rows;
    }

    /**
     * Write to a MongoDB collection.
     * @param {string} connectionString   MongoDB URI (e.g. "mongodb://user:pass@host:27017").
     * @param {string} dbDotCollection    Target as "database.collection".
     * @param {string} mode               'append' (default) or 'replace'.
     * @returns {number} Rows written.
     */
    toMongodb(connectionString, dbDotCollection, mode = 'append') {
        if (!['append', 'replace'].includes(mode)) {
            throw new Error("mode must be 'append' or 'replace'.");
        }
        const lib  = getLib();
        const rows = lib.pardox_write_mongodb(
            this[PTR_SYMBOL],
            connectionString,
            dbDotCollection,
            mode
        );
        if (rows < 0) {
            throw new Error(`toMongodb failed with error code: ${rows}`);
        }
        return rows;
    }

    // -------------------------------------------------------------------------
    // Observer v0.3.1 — Native Universal Export & Vectorized Inspection
    // -------------------------------------------------------------------------

    /**
     * Returns ALL rows as an array of objects (records format).
     * Equivalent to Pandas' df.to_dict('records').
     * @returns {Object[]}
     */
    toDict() {
        const lib  = getLib();
        const json = lib.pardox_to_json_records(this[PTR_SYMBOL]);
        if (!json) return [];
        try { return JSON.parse(json); } catch { return []; }
    }

    /**
     * Returns ALL rows as an array of arrays (values format).
     * Equivalent to Pandas' df.values.tolist().
     * @returns {Array[]}
     */
    toList() {
        const lib  = getLib();
        const json = lib.pardox_to_json_arrays(this[PTR_SYMBOL]);
        if (!json) return [];
        try { return JSON.parse(json); } catch { return []; }
    }

    /**
     * Serializes ALL rows to a JSON string (records format, no row limit).
     * Unlike the preview toJson(limit), this exports the full DataFrame.
     * @returns {string}
     */
    toJson() {
        const lib  = getLib();
        const json = lib.pardox_to_json_records(this[PTR_SYMBOL]);
        return json || '[]';
    }

    /**
     * Returns the frequency of each unique value in the given column.
     * @param {string} col  Column name.
     * @returns {Object}  { "value": count, ... } sorted by count descending.
     */
    valueCounts(col) {
        const lib  = getLib();
        const json = lib.pardox_value_counts(this[PTR_SYMBOL], col);
        if (!json) return {};
        try { return JSON.parse(json); } catch { return {}; }
    }

    /**
     * Returns the unique values of the given column in insertion order.
     * @param {string} col  Column name.
     * @returns {Array}  Flat array of unique values.
     */
    unique(col) {
        const lib  = getLib();
        const json = lib.pardox_unique(this[PTR_SYMBOL], col);
        if (!json) return [];
        try { return JSON.parse(json); } catch { return []; }
    }

    // -------------------------------------------------------------------------
    // Native Math Foundation (v0.3.1)
    // -------------------------------------------------------------------------

    /**
     * Columnar addition: col_a + col_b → new DataFrame with "result_math_add".
     * @param {string} colA
     * @param {string} colB
     * @returns {DataFrame}
     */
    add(colA, colB) {
        const lib = getLib();
        const ptr = lib.pardox_math_add(this[PTR_SYMBOL], colA, colB);
        if (!ptr) throw new Error('pardox_math_add returned null.');
        return new DataFrame(ptr);
    }

    /**
     * Columnar subtraction: col_a - col_b → new DataFrame with "result_math_sub".
     * @param {string} colA
     * @param {string} colB
     * @returns {DataFrame}
     */
    sub(colA, colB) {
        const lib = getLib();
        const ptr = lib.pardox_math_sub(this[PTR_SYMBOL], colA, colB);
        if (!ptr) throw new Error('pardox_math_sub returned null.');
        return new DataFrame(ptr);
    }

    /**
     * Columnar multiplication: col_a * col_b → new DataFrame with "result_mul".
     * @param {string} colA
     * @param {string} colB
     * @returns {DataFrame}
     */
    mul(colA, colB) {
        const lib = getLib();
        const ptr = lib.pardox_series_mul(this[PTR_SYMBOL], colA, this[PTR_SYMBOL], colB);
        if (!ptr) throw new Error('pardox_series_mul returned null.');
        return new DataFrame(ptr);
    }

    /**
     * Native sample standard deviation of a column.
     * @param {string} col
     * @returns {number}
     */
    std(col) {
        const lib = getLib();
        return lib.pardox_math_stddev(this[PTR_SYMBOL], col);
    }

    /**
     * Min-Max Scaler: normalizes column values to [0, 1].
     * Returns new DataFrame with column "result_minmax".
     * @param {string} col
     * @returns {DataFrame}
     */
    minMaxScale(col) {
        const lib = getLib();
        const ptr = lib.pardox_math_minmax(this[PTR_SYMBOL], col);
        if (!ptr) throw new Error('pardox_math_minmax returned null.');
        return new DataFrame(ptr);
    }

    /**
     * Sort rows by the named column.
     * @param {string}  by        Column name to sort by.
     * @param {boolean} ascending Sort order (default true).
     * @param {boolean} gpu       Use GPU Bitonic sort pipeline (falls back to CPU).
     * @returns {DataFrame}
     */
    sortValues(by, ascending = true, gpu = false) {
        const lib = getLib();
        let ptr;
        if (gpu) {
            ptr = lib.pardox_gpu_sort(this[PTR_SYMBOL], by);
        } else {
            const descending = ascending ? 0 : 1;
            ptr = lib.pardox_sort_values(this[PTR_SYMBOL], by, descending);
        }
        if (!ptr) throw new Error('Sort returned null.');
        return new DataFrame(ptr);
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
        const lib = getLib();
        const newPtr = lib.pardox_slice_manager(this[PTR_SYMBOL], start, length);
        if (!newPtr) throw new Error('Slice operation returned null.');
        return new DataFrame(newPtr);
    }

    // Internal: assign a Series result as a column
    _setColumn(colName, series) {
        const { Series } = require('./Series');
        if (!(series instanceof Series)) {
            throw new TypeError('Column assignment requires a PardoX Series.');
        }

        const lib = getLib();
        const res = lib.pardox_add_column(this[PTR_SYMBOL], series._parentPtr(), colName);

        if (res !== 1) {
            const errors = { '-1': 'Invalid Pointers', '-2': 'Invalid Column Name', '-3': 'Engine Logic Error' };
            throw new Error(`Column assignment failed: ${errors[String(res)] || `Unknown: ${res}`}`);
        }
    }
}

// -------------------------------------------------------------------------
// Proxy factory — enables df['col'] and df['col'] = series
// -------------------------------------------------------------------------

// Properties that belong to the DataFrame class itself (not column names)
const OWN_PROPS = new Set([
    'shape', 'columns', 'dtypes',
    'head', 'tail', 'show', 'toJson', 'iloc',
    'cast', 'join', 'filter', 'fillna', 'round',
    'toCsv', 'toPrdx', 'toSql', 'toMysql', 'toSqlserver', 'toMongodb',
    'toDict', 'toList', 'toJson', 'valueCounts', 'unique',
    'add', 'sub', 'std', 'minMaxScale', 'sortValues',
    'read_csv', 'read_sql', 'read_mysql', 'read_sqlserver', 'read_mongodb', 'fromArray',
    '_free', '_schema', 'slice', '_setColumn',
    'constructor', 'then',        // 'then' prevents Proxy from being treated as a thenable
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
