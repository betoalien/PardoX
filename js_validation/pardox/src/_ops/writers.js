'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /** Export to CSV. */
    toCsv(path) {
        const lib = getLib();
        const res = lib.pardox_to_csv(this._ptr, path);
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
    },

    /** Export to native PardoX binary format (.prdx). */
    toPrdx(path) {
        const lib = getLib();
        const res = lib.pardox_to_prdx(this._ptr, path);
        if (res !== 1) throw new Error(`toPrdx failed with code: ${res}`);
        return true;
    },

    /**
     * Write to a PostgreSQL table.
     * @param {string}   connectionString
     * @param {string}   tableName
     * @param {string}   mode              'append' (default) or 'upsert'.
     * @param {string[]} conflictCols
     * @returns {number} Rows written.
     */
    toSql(connectionString, tableName, mode = 'append', conflictCols = []) {
        if (!['append', 'upsert'].includes(mode)) {
            throw new Error("mode must be 'append' or 'upsert'.");
        }
        const lib = getLib();
        const colsJson = JSON.stringify(conflictCols);
        const rows = lib.pardox_write_sql(this._ptr, connectionString, tableName, mode, colsJson);
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
    },

    /**
     * Write to a MySQL table.
     * @param {string}   connectionString
     * @param {string}   tableName
     * @param {string}   mode
     * @param {string[]} conflictCols
     * @returns {number}
     */
    toMysql(connectionString, tableName, mode = 'append', conflictCols = []) {
        if (!['append', 'replace', 'upsert'].includes(mode)) {
            throw new Error("mode must be 'append', 'replace', or 'upsert'.");
        }
        const lib = getLib();
        const rows = lib.pardox_write_mysql(this._ptr, connectionString, tableName, mode, JSON.stringify(conflictCols));
        if (rows < 0) throw new Error(`toMysql failed with error code: ${rows}`);
        return rows;
    },

    /**
     * Write to a SQL Server table.
     * @param {string}   connectionString
     * @param {string}   tableName
     * @param {string}   mode
     * @param {string[]} conflictCols
     * @returns {number}
     */
    toSqlserver(connectionString, tableName, mode = 'append', conflictCols = []) {
        if (!['append', 'replace', 'upsert'].includes(mode)) {
            throw new Error("mode must be 'append', 'replace', or 'upsert'.");
        }
        const lib = getLib();
        const rows = lib.pardox_write_sqlserver(this._ptr, connectionString, tableName, mode, JSON.stringify(conflictCols));
        if (rows < 0) throw new Error(`toSqlserver failed with error code: ${rows}`);
        return rows;
    },

    /**
     * Write to a MongoDB collection.
     * @param {string} connectionString
     * @param {string} dbDotCollection
     * @param {string} mode
     * @returns {number}
     */
    toMongodb(connectionString, dbDotCollection, mode = 'append') {
        if (!['append', 'replace'].includes(mode)) {
            throw new Error("mode must be 'append' or 'replace'.");
        }
        const lib = getLib();
        const rows = lib.pardox_write_mongodb(this._ptr, connectionString, dbDotCollection, mode);
        if (rows < 0) throw new Error(`toMongodb failed with error code: ${rows}`);
        return rows;
    },

    /**
     * Write the DataFrame to a Parquet file.
     * @param {string} path
     * @returns {number}
     */
    toParquet(path) {
        const lib = getLib();
        const result = lib.pardox_to_parquet(this._ptr, path);
        if (result < 0) throw new Error(`toParquet failed with code: ${result}`);
        return result;
    },

    /**
     * Write the DataFrame to multiple sharded Parquet files.
     * @param {string} directory
     * @param {number} maxRowsPerShard
     * @returns {number}
     */
    writeShardedParquet(directory, maxRowsPerShard = 1_000_000) {
        const lib = getLib();
        const result = lib.pardox_write_sharded_parquet(this._ptr, directory, maxRowsPerShard);
        if (result < 0) throw new Error(`writeShardedParquet failed with code: ${result}`);
        return result;
    },
};
