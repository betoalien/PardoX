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

    /**
     * Public method to free the underlying Rust manager.
     * Call this explicitly when done with a DataFrame to avoid memory leaks.
     */
    free() {
        this._free();
    }

    /**
     * Public method to free the underlying Rust memory.
     * Call this when done with a DataFrame to avoid memory leaks.
     */
    free() {
        this._free();
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
     * @param {boolean}     usePipe Optional: Use pipe delimiter (for JSON columns). Default: false
     * @returns {DataFrame}
     */
    static read_csv(path, schema = null, usePipe = false) {
        const lib = getLib();
        const fs = require('fs');

        if (!fs.existsSync(path)) {
            throw new Error(`CSV file not found: ${path}`);
        }

        let schemaJson = '{}';
        if (schema !== null) {
            // Convert { colName: 'Int64', ... } to { column_names: [...], column_types: [...] }
            // This is the format expected by the Rust core
            const entries = Object.entries(schema);
            const column_names = entries.map(([name, _]) => name);
            const column_types = entries.map(([_, type]) => type);
            schemaJson = JSON.stringify({ column_names, column_types });
        }

        const config = usePipe ? PIPE_CSV_CONFIG : DEFAULT_CSV_CONFIG;
        const ptr = lib.pardox_load_manager_csv(path, schemaJson, config);

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

    // -------------------------------------------------------------------------
    // Gap 1: GroupBy Aggregation
    // -------------------------------------------------------------------------

    /**
     * GroupBy aggregation.
     * @param {string|string[]} cols  Column name(s) to group by.
     * @param {Object} agg            Aggregation dict: { colName: 'sum'|'mean'|'count'|'min'|'max'|'std' }
     * @returns {DataFrame}
     */
    groupBy(cols, agg) {
        const lib = getLib();
        const colsJson = JSON.stringify(Array.isArray(cols) ? cols : [cols]);
        const aggJson = JSON.stringify(agg);
        const ptr = lib.pardox_groupby_agg(this[PTR_SYMBOL], colsJson, aggJson);
        if (!ptr) throw new Error(`groupBy failed: cols=${cols} agg=${JSON.stringify(agg)}`);
        return new DataFrame(ptr);
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
    // Gap 7: GPU Compute
    // -------------------------------------------------------------------------

    /**
     * GPU-accelerated column addition: col_a + col_b → "gpu_add".
     * @param {string} colA
     * @param {string} colB
     * @returns {DataFrame}
     */
    gpuAdd(colA, colB) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_add(this[PTR_SYMBOL], colA, colB);
        if (!ptr) throw new Error('pardox_gpu_add returned null.');
        return new DataFrame(ptr);
    }

    /**
     * GPU-accelerated column subtraction: col_a - col_b → "gpu_sub".
     * @param {string} colA
     * @param {string} colB
     * @returns {DataFrame}
     */
    gpuSub(colA, colB) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_sub(this[PTR_SYMBOL], colA, colB);
        if (!ptr) throw new Error('pardox_gpu_sub returned null.');
        return new DataFrame(ptr);
    }

    /**
     * GPU-accelerated column multiplication: col_a * col_b → "gpu_mul".
     * @param {string} colA
     * @param {string} colB
     * @returns {DataFrame}
     */
    gpuMul(colA, colB) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_mul(this[PTR_SYMBOL], colA, colB);
        if (!ptr) throw new Error('pardox_gpu_mul returned null.');
        return new DataFrame(ptr);
    }

    /**
     * GPU-accelerated column division: col_a / col_b → "gpu_div".
     * @param {string} colA
     * @param {string} colB
     * @returns {DataFrame}
     */
    gpuDiv(colA, colB) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_div(this[PTR_SYMBOL], colA, colB);
        if (!ptr) throw new Error('pardox_gpu_div returned null.');
        return new DataFrame(ptr);
    }

    /**
     * GPU-accelerated square root: sqrt(col) → "gpu_sqrt".
     * @param {string} col
     * @returns {DataFrame}
     */
    gpuSqrt(col) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_sqrt(this[PTR_SYMBOL], col);
        if (!ptr) throw new Error('pardox_gpu_sqrt returned null.');
        return new DataFrame(ptr);
    }

    /**
     * GPU-accelerated exponential: exp(col) → "gpu_exp".
     * @param {string} col
     * @returns {DataFrame}
     */
    gpuExp(col) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_exp(this[PTR_SYMBOL], col);
        if (!ptr) throw new Error('pardox_gpu_exp returned null.');
        return new DataFrame(ptr);
    }

    /**
     * GPU-accelerated natural logarithm: log(col) → "gpu_log".
     * @param {string} col
     * @returns {DataFrame}
     */
    gpuLog(col) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_log(this[PTR_SYMBOL], col);
        if (!ptr) throw new Error('pardox_gpu_log returned null.');
        return new DataFrame(ptr);
    }

    /**
     * GPU-accelerated absolute value: abs(col) → "gpu_abs".
     * @param {string} col
     * @returns {DataFrame}
     */
    gpuAbs(col) {
        const lib = getLib();
        const ptr = lib.pardox_gpu_abs(this[PTR_SYMBOL], col);
        if (!ptr) throw new Error('pardox_gpu_abs returned null.');
        return new DataFrame(ptr);
    }

    // -------------------------------------------------------------------------
    // Gap 8: Pivot & Melt
    // -------------------------------------------------------------------------

    /**
     * Pivot table transformation.
     * @param {string} index     Column to use as index.
     * @param {string} columns   Column to pivot (unique values become new columns).
     * @param {string} values    Column with values to fill the pivot table.
     * @param {string} aggFunc   Aggregation function: 'sum', 'mean', 'count', 'min', 'max'.
     * @returns {DataFrame}
     */
    pivotTable(index, columns, values, aggFunc = 'sum') {
        const lib = getLib();
        const ptr = lib.pardox_pivot_table(this[PTR_SYMBOL], index, columns, values, aggFunc);
        if (!ptr) throw new Error('pardox_pivot_table returned null. Check column names and agg_func.');
        return new DataFrame(ptr);
    }

    /**
     * Melt (unpivot) transformation.
     * @param {string[]} idVars     Columns to keep as identifiers.
     * @param {string[]} valueVars  Columns to unpivot into rows.
     * @param {string}   varName    Name for the 'variable' column (default: 'variable').
     * @param {string}   valueName  Name for the 'value' column (default: 'value').
     * @returns {DataFrame}
     */
    melt(idVars, valueVars, varName = 'variable', valueName = 'value') {
        const lib = getLib();
        const idVarsJson = JSON.stringify(Array.isArray(idVars) ? idVars : [idVars]);
        const valueVarsJson = JSON.stringify(Array.isArray(valueVars) ? valueVars : [valueVars]);
        const ptr = lib.pardox_melt(this[PTR_SYMBOL], idVarsJson, valueVarsJson, varName, valueName);
        if (!ptr) throw new Error('pardox_melt returned null. Check column names.');
        return new DataFrame(ptr);
    }

    // -------------------------------------------------------------------------
    // Gap 9: Time Series Fill
    // -------------------------------------------------------------------------

    /**
     * Forward fill null values in a column.
     * @param {string} col  Column name.
     * @returns {DataFrame} New DataFrame with forward-filled column.
     */
    ffill(col) {
        const lib = getLib();
        const ptr = lib.pardox_ffill(this[PTR_SYMBOL], col);
        if (!ptr) throw new Error('pardox_ffill returned null.');
        return new DataFrame(ptr);
    }

    /**
     * Backward fill null values in a column.
     * @param {string} col  Column name.
     * @returns {DataFrame} New DataFrame with backward-filled column.
     */
    bfill(col) {
        const lib = getLib();
        const ptr = lib.pardox_bfill(this[PTR_SYMBOL], col);
        if (!ptr) throw new Error('pardox_bfill returned null.');
        return new DataFrame(ptr);
    }

    /**
     * Linear interpolation of null values in a column.
     * @param {string} col  Column name.
     * @returns {DataFrame} New DataFrame with interpolated column.
     */
    interpolate(col) {
        const lib = getLib();
        const ptr = lib.pardox_interpolate(this[PTR_SYMBOL], col);
        if (!ptr) throw new Error('pardox_interpolate returned null.');
        return new DataFrame(ptr);
    }

    // -------------------------------------------------------------------------
    // Gap 10: Nested Data
    // -------------------------------------------------------------------------

    /**
     * Extract a value from a JSON column by key.
     * @param {string} col  JSON column name.
     * @param {string} key  JSON key to extract.
     * @returns {DataFrame} New DataFrame with extracted values.
     */
    jsonExtract(col, key) {
        const lib = getLib();
        const ptr = lib.pardox_json_extract(this[PTR_SYMBOL], col, key);
        if (!ptr) throw new Error('pardox_json_extract returned null.');
        return new DataFrame(ptr);
    }

    /**
     * Explode a list-like column, creating one row per element.
     * @param {string} col  Column to explode.
     * @returns {DataFrame} New DataFrame with exploded rows.
     */
    explode(col) {
        const lib = getLib();
        const ptr = lib.pardox_explode(this[PTR_SYMBOL], col);
        if (!ptr) throw new Error('pardox_explode returned null.');
        return new DataFrame(ptr);
    }

    /**
     * Unnest a struct column, expanding its fields into separate columns.
     * @param {string} col  Struct column to unnest.
     * @returns {DataFrame} New DataFrame with unnested columns.
     */
    unnest(col) {
        const lib = getLib();
        const ptr = lib.pardox_unnest(this[PTR_SYMBOL], col);
        if (!ptr) throw new Error('pardox_unnest returned null.');
        return new DataFrame(ptr);
    }

    // -------------------------------------------------------------------------
    // Gap 11: Spill to Disk
    // -------------------------------------------------------------------------

    /**
     * Spill DataFrame to disk in .prdx format.
     * @param {string} path  File path to write.
     * @returns {number} 1 on success, throws on error.
     */
    spillToDisk(path) {
        const lib = getLib();
        const result = lib.pardox_spill_to_disk(this[PTR_SYMBOL], path);
        // pardox_spill_to_disk returns bytes written (>0) on success, negative on error
        if (result <= 0) {
            throw new Error(`pardox_spill_to_disk failed with error code: ${result}`);
        }
        return 1;  // Return 1 to indicate success (matches Python/PHP SDK behavior)
    }

    /**
     * Load a spilled DataFrame from disk.
     * @param {string} path  File path to read.
     * @returns {DataFrame}
     */
    static spillFromDisk(path) {
        const lib = getLib();
        const ptr = lib.pardox_spill_from_disk(path);
        if (!ptr) throw new Error('pardox_spill_from_disk returned null.');
        return new DataFrame(ptr);
    }

    /**
     * Streaming GroupBy with chunked processing for large datasets.
     * @param {string} groupCol   Column to group by.
     * @param {Object} aggSpecs   Aggregation specs: { col: 'sum'|'mean'|'count'|'min'|'max' }.
     * @param {number} chunkSize  Number of rows per chunk (default: 100000).
     * @returns {DataFrame}
     */
    chunkedGroupby(groupCol, aggSpecs, chunkSize = 100000) {
        const lib = getLib();
        const aggJson = JSON.stringify(aggSpecs);
        const ptr = lib.pardox_chunked_groupby(this[PTR_SYMBOL], groupCol, aggJson, chunkSize);
        if (!ptr) throw new Error('pardox_chunked_groupby returned null.');
        return new DataFrame(ptr);
    }

    /**
     * External sort for large datasets that don't fit in memory.
     * @param {string} by         Column to sort by.
     * @param {boolean} ascending Sort order (default true).
     * @param {number} chunkSize  Number of rows per chunk (default: 100000).
     * @returns {DataFrame}
     */
    externalSort(by, ascending = true, chunkSize = 100000) {
        const lib = getLib();
        const ascendingInt = ascending ? 1 : 0;
        const ptr = lib.pardox_external_sort(this[PTR_SYMBOL], by, ascendingInt, chunkSize);
        if (!ptr) throw new Error('pardox_external_sort returned null.');
        return new DataFrame(ptr);
    }

    /**
     * Get memory usage of this DataFrame in bytes.
     * @returns {number} Memory usage in bytes.
     */
    memoryUsage() {
        const lib = getLib();
        return lib.pardox_memory_usage(this[PTR_SYMBOL]);
    }

    /**
     * Get current process memory usage in bytes (static).
     * @returns {number} Memory usage in bytes.
     */
    static memoryUsage() {
        const lib = getLib();
        return lib.pardox_memory_usage(null);
    }

    // -------------------------------------------------------------------------
    // Gap 14: SQL Query over in-memory DataFrame
    // -------------------------------------------------------------------------

    /**
     * Execute a SQL SELECT query on this DataFrame.
     *
     * Supported SQL subset:
     *   SELECT * | col [AS alias] | AGG(col) [AS alias]
     *   FROM table_name (ignored — queries the current DataFrame)
     *   WHERE col OP value (AND / OR / NOT / BETWEEN / LIKE / IN)
     *   GROUP BY col [, col ...]
     *   HAVING agg_cond
     *   ORDER BY col [ASC | DESC]
     *   LIMIT n
     *
     * Aggregates: SUM, COUNT, AVG/MEAN, MIN, MAX, STD
     *
     * @param {string} sql The SQL query to execute.
     * @returns {DataFrame|null} New DataFrame with query results, or null on error.
     */
    sql(sqlQuery) {
        const lib = getLib();
        const ptr = lib.pardox_sql_query(this[PTR_SYMBOL], sqlQuery);
        if (!ptr) return null;
        return new DataFrame(ptr);
    }

    // -------------------------------------------------------------------------
    // Gap 14: SQL Query over in-memory DataFrame
    // -------------------------------------------------------------------------

    /**
     * Execute a SQL SELECT query on this DataFrame.
     *
     * Supported SQL subset:
     *   SELECT * | col [AS alias] | AGG(col) [AS alias]
     *   FROM table_name (ignored — queries the current DataFrame)
     *   WHERE col OP value (AND / OR / NOT / BETWEEN / LIKE / IN)
     *   GROUP BY col [, col ...]
     *   HAVING agg_cond
     *   ORDER BY col [ASC | DESC]
     *   LIMIT n
     *
     * Aggregates: SUM, COUNT, AVG/MEAN, MIN, MAX, STD
     *
     * @param {string} sql The SQL query to execute.
     * @returns {DataFrame|null} New DataFrame with results, or null on error.
     */
    sql(sqlQuery) {
        const lib = getLib();
        const ptr = lib.pardox_sql_query(this[PTR_SYMBOL], sqlQuery);
        if (!ptr) return null;
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

    // =========================================================================
    // GAP 19: DATA CONTRACTS
    // =========================================================================

    /**
     * Validate the DataFrame against a data contract.
     * @param {Object} rules - Validation rules, e.g. { amount: 'not_null' }
     * @returns {number} Number of violations (0 = all passed)
     */
    validateContract(rules) {
        const lib = getLib();
        return lib.pardox_validate_contract(this[PTR_SYMBOL], JSON.stringify(rules));
    }

    /**
     * Return the number of violations from the last validateContract() call.
     * @returns {number}
     */
    static contractViolationCount() {
        const lib = getLib();
        return lib.pardox_contract_violation_count();
    }

    // =========================================================================
    // GAP 20: TIME TRAVEL
    // =========================================================================

    /**
     * Write a versioned snapshot of this DataFrame to disk.
     * @param {string} path      Base directory path.
     * @param {string} label     Version label.
     * @param {number} timestamp Optional Unix timestamp (0 = current time).
     * @returns {number} Rows written.
     */
    versionWrite(path, label, timestamp = 0) {
        const lib = getLib();
        const result = lib.pardox_version_write(this[PTR_SYMBOL], path, label, timestamp);
        if (result < 0) throw new Error(`versionWrite failed with code: ${result}`);
        return result;
    }

    // =========================================================================
    // GAP 28: LINEAR ALGEBRA
    // =========================================================================

    /**
     * L2-normalize a numeric column (unit vector).
     * @param {string} col Column name.
     * @returns {DataFrame} New DataFrame.
     */
    linalgL2Normalize(col) {
        const lib = getLib();
        const ptr = lib.pardox_l2_normalize(this[PTR_SYMBOL], col);
        if (!ptr) throw new Error('linalgL2Normalize returned null.');
        return new DataFrame(ptr);
    }

    /**
     * L1-normalize a numeric column (sum-to-one).
     * @param {string} col Column name.
     * @returns {DataFrame} New DataFrame.
     */
    linalgL1Normalize(col) {
        const lib = getLib();
        const ptr = lib.pardox_l1_normalize(this[PTR_SYMBOL], col);
        if (!ptr) throw new Error('linalgL1Normalize returned null.');
        return new DataFrame(ptr);
    }

    /**
     * Cosine similarity between two columns across two DataFrames.
     * @param {string}    colA   Column in this DataFrame.
     * @param {DataFrame} other  The other DataFrame.
     * @param {string}    colB   Column in other.
     * @returns {number} Cosine similarity score.
     */
    linalgCosineSim(colA, other, colB) {
        const lib = getLib();
        return lib.pardox_cosine_sim(this[PTR_SYMBOL], colA, other[PTR_SYMBOL], colB);
    }

    /**
     * PCA — reduce a column to N principal components.
     * @param {string} col         Column name.
     * @param {number} nComponents Number of components.
     * @returns {DataFrame} New DataFrame.
     */
    linalgPca(col, nComponents) {
        const lib = getLib();
        const ptr = lib.pardox_pca(this[PTR_SYMBOL], col, nComponents);
        if (!ptr) throw new Error('linalgPca returned null.');
        return new DataFrame(ptr);
    }

    /**
     * Matrix multiplication between columns of two DataFrames.
     * @param {string}    colA  Column in this DataFrame.
     * @param {DataFrame} other Other DataFrame.
     * @param {string}    colB  Column in other.
     * @returns {DataFrame} New DataFrame with matmul result.
     */
    linalgMatmul(colA, other, colB) {
        const lib = getLib();
        const ptr = lib.pardox_matmul(this[PTR_SYMBOL], colA, other[PTR_SYMBOL], colB);
        if (!ptr) throw new Error('linalgMatmul returned null.');
        return new DataFrame(ptr);
    }

    // =========================================================================
    // PARQUET SUPPORT
    // =========================================================================

    /**
     * Write the DataFrame to a Parquet file.
     * @param {string} path Destination file path.
     * @returns {number} Rows written.
     */
    toParquet(path) {
        const lib = getLib();
        const result = lib.pardox_to_parquet(this[PTR_SYMBOL], path);
        if (result < 0) throw new Error(`toParquet failed with code: ${result}`);
        return result;
    }

    /**
     * Write the DataFrame to multiple sharded Parquet files.
     * @param {string} directory       Output directory path.
     * @param {number} maxRowsPerShard Max rows per shard (default 1,000,000).
     * @returns {number} Rows written.
     */
    writeShardedParquet(directory, maxRowsPerShard = 1_000_000) {
        const lib = getLib();
        const result = lib.pardox_write_sharded_parquet(this[PTR_SYMBOL], directory, maxRowsPerShard);
        if (result < 0) throw new Error(`writeShardedParquet failed with code: ${result}`);
        return result;
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
    '_free', 'free', '_schema', 'slice', '_setColumn',
    'constructor', 'then',        // 'then' prevents Proxy from being treated as a thenable
    // Gap 7: GPU Compute
    'gpuAdd', 'gpuSub', 'gpuMul', 'gpuDiv', 'gpuSqrt', 'gpuExp', 'gpuLog', 'gpuAbs',
    // Gap 8: Pivot & Melt
    'pivotTable', 'melt',
    // Gap 9: Time Series Fill
    'ffill', 'bfill', 'interpolate',
    // Gap 10: Nested Data
    'jsonExtract', 'explode', 'unnest',
    // Gap 11: Spill to Disk
    'spillToDisk', 'spillFromDisk', 'chunkedGroupby', 'externalSort', 'memoryUsage',
    // Gap 19: Data Contracts
    'validateContract', 'contractViolationCount',
    // Gap 20: Time Travel
    'versionWrite',
    // Gap 28: Linear Algebra
    'linalgL2Normalize', 'linalgL1Normalize', 'linalgCosineSim', 'linalgPca', 'linalgMatmul',
    // Parquet
    'toParquet', 'writeShardedParquet',
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
