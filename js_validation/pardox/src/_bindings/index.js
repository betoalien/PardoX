'use strict';

const koffi = require('koffi');

const bindCore        = require('./core');
const bindInspection  = require('./inspection');
const bindCompute     = require('./compute');
const bindMutation    = require('./mutation');
const bindObserver    = require('./observer');
const bindMathOps     = require('./math_ops');
const bindStrings     = require('./strings');
const bindDatetime    = require('./datetime');
const bindDecimal     = require('./decimal');
const bindWindow      = require('./window');
const bindLazy        = require('./lazy_bindings');
const bindGpu         = require('./gpu');
const bindGroupby     = require('./groupby');
const bindPrdxIo      = require('./prdx_io');
const bindPersistence = require('./persistence');
const bindSqlQuery    = require('./sql_query');
const bindDatabases   = require('./databases');
const bindEncryption  = require('./encryption');
const bindContracts   = require('./contracts');
const bindTimetravel  = require('./timetravel');
const bindReshape     = require('./reshape');
const bindTimeseries  = require('./timeseries');
const bindSpill       = require('./spill');
const bindCloud       = require('./cloud');
const bindFlight      = require('./flight');
const bindCluster     = require('./cluster');
const bindLiveQuery   = require('./live_query');
const bindRest        = require('./rest');
const bindLinalg      = require('./linalg');
const bindMisc        = require('./misc');

/**
 * Read a null-terminated C string from an opaque pointer and free it.
 * Use ONLY for functions that return heap-allocated strings via CString::into_raw().
 * @param {*}        ptr      koffi opaque pointer (void *)
 * @param {Function} freeFn   The pardox_free_string function
 * @returns {string|null}
 */
function _readAndFreeStr(ptr, freeFn) {
    if (!ptr) return null;
    const str = koffi.decode(ptr, 'string');
    freeFn(ptr);
    return str;
}

/**
 * Registers all raw koffi function bindings onto _lib, then wraps them
 * with the same convenience wrappers as the original ffi.js.
 *
 * @param {Object} _lib  The library object. _lib.raw is the koffi handle.
 */
module.exports = function bindAll(_lib) {
    // Register all raw bindings first (each module writes to _lib directly)
    bindCore(_lib);
    bindInspection(_lib);
    bindCompute(_lib);
    bindMutation(_lib);
    bindObserver(_lib);
    bindMathOps(_lib);
    bindStrings(_lib);
    bindDatetime(_lib);
    bindDecimal(_lib);
    bindWindow(_lib);
    bindLazy(_lib);
    bindGpu(_lib);
    bindGroupby(_lib);
    bindPrdxIo(_lib);
    bindPersistence(_lib);
    bindSqlQuery(_lib);
    bindDatabases(_lib);
    bindEncryption(_lib);
    bindContracts(_lib);
    bindTimetravel(_lib);
    bindReshape(_lib);
    bindTimeseries(_lib);
    bindSpill(_lib);
    bindCloud(_lib);
    bindFlight(_lib);
    bindCluster(_lib);
    bindLiveQuery(_lib);
    bindRest(_lib);
    bindLinalg(_lib);
    bindMisc(_lib);

    // -------------------------------------------------------------------------
    // Wrap raw bindings with the same convenience wrappers from original ffi.js
    // -------------------------------------------------------------------------

    const free = _lib.pardox_free_string;

    // Inspection wrappers
    _lib.pardox_get_row_count         = (mgr) => Number(_lib._raw_pardox_get_row_count(mgr));
    _lib.pardox_get_schema_json       = (mgr) => _lib._raw_pardox_get_schema_json(mgr) || null;
    _lib.pardox_manager_to_json       = (mgr, limit) => _lib._raw_pardox_manager_to_json(mgr, limit) || null;
    _lib.pardox_manager_to_ascii      = (mgr, limit) => _lib._raw_pardox_manager_to_ascii(mgr, limit) || null;
    _lib.pardox_manager_to_json_range = (mgr, start, limit) => _lib._raw_pardox_manager_to_json_range(mgr, start, limit) || null;
    _lib.pardox_read_head_json        = (path, limit) => _lib._raw_pardox_read_head_json(path, limit) || null;

    // Mutation wrappers
    _lib.pardox_add_column = (target, source, col) => Number(_lib._raw_pardox_add_column(target, source, col));
    _lib.pardox_fill_na    = (mgr, col, val) => Number(_lib._raw_pardox_fill_na(mgr, col, val));
    _lib.pardox_round      = (mgr, col, dec) => Number(_lib._raw_pardox_round(mgr, col, dec));

    // Observer wrappers (heap-allocated strings: decode + free)
    _lib.pardox_to_json_records = (mgr) => _readAndFreeStr(_lib._raw_pardox_to_json_records(mgr), free);
    _lib.pardox_to_json_arrays  = (mgr) => _readAndFreeStr(_lib._raw_pardox_to_json_arrays(mgr), free);
    _lib.pardox_value_counts    = (mgr, col) => _readAndFreeStr(_lib._raw_pardox_value_counts(mgr, col), free);
    _lib.pardox_unique          = (mgr, col) => _readAndFreeStr(_lib._raw_pardox_unique(mgr, col), free);

    // Writer wrappers
    _lib.pardox_to_csv              = (mgr, path) => Number(_lib._raw_pardox_to_csv(mgr, path));
    _lib.pardox_to_prdx             = (mgr, path) => Number(_lib._raw_pardox_to_prdx(mgr, path));
    _lib.pardox_to_parquet          = (mgr, path) => Number(_lib._raw_pardox_to_parquet(mgr, path));
    _lib.pardox_write_sharded_parquet = (mgr, dir, maxRows) => Number(_lib._raw_pardox_write_sharded_parquet(mgr, dir, maxRows));

    // SQL wrappers
    _lib.pardox_write_sql   = (mgr, conn, table, mode, cols) => Number(_lib._raw_pardox_write_sql(mgr, conn, table, mode, cols));
    _lib.pardox_execute_sql = (conn, query) => Number(_lib._raw_pardox_execute_sql(conn, query));

    _lib.pardox_write_sql_prdx = (prdxPath, conn, table, mode, conflictCols, batchRows) => {
        try {
            const fn = _lib.raw.func('int64_t pardox_write_sql_prdx(const char * prdx_path, const char * conn_str, const char * table_name, const char * mode, const char * conflict_cols_json, int64_t batch_rows)');
            return Number(fn(prdxPath, conn, table, mode, conflictCols, batchRows));
        } catch (e) {
            return -1;
        }
    };

    // Database wrappers
    _lib.pardox_write_mysql      = (mgr, conn, table, mode, cols) => Number(_lib._raw_pardox_write_mysql(mgr, conn, table, mode, cols));
    _lib.pardox_execute_mysql    = (conn, query) => Number(_lib._raw_pardox_execute_mysql(conn, query));
    _lib.pardox_write_sqlserver  = (mgr, conn, table, mode, cols) => Number(_lib._raw_pardox_write_sqlserver(mgr, conn, table, mode, cols));
    _lib.pardox_execute_sqlserver = (conn, query) => Number(_lib._raw_pardox_execute_sqlserver(conn, query));
    _lib.pardox_write_mongodb    = (mgr, conn, dbColl, mode) => Number(_lib._raw_pardox_write_mongodb(mgr, conn, dbColl, mode));
    _lib.pardox_execute_mongodb  = (conn, db, cmdJson) => Number(_lib._raw_pardox_execute_mongodb(conn, db, cmdJson));

    // Cloud wrappers
    _lib.pardox_cloud_write_prdx = (mgr, url, creds) => Number(_lib._raw_pardox_cloud_write_prdx(mgr, url, creds));

    // Decimal wrappers
    _lib.pardox_decimal_sum = (mgr, col) => Number(_lib._raw_pardox_decimal_sum(mgr, col));

    // Spill wrappers
    _lib.pardox_spill_to_disk = (mgr, path) => Number(_lib._raw_pardox_spill_to_disk(mgr, path));
    _lib.pardox_memory_usage  = (mgr) => Number(_lib._raw_pardox_memory_usage(mgr));

    // PRDX wrappers
    _lib.pardox_prdx_count          = (path) => Number(_lib._raw_pardox_prdx_count(path));
    _lib.pardox_read_schema         = (path) => _lib._raw_pardox_read_schema(path) || null;
    _lib.pardox_inspect_file_lazy   = (path, opts) => _lib._raw_pardox_inspect_file_lazy(path, opts) || null;

    // Lazy wrappers
    _lib.pardox_lazy_describe = (frame) => _lib._raw_pardox_lazy_describe(frame) || null;
    _lib.pardox_lazy_stats    = (frame) => _lib._raw_pardox_lazy_stats(frame) || null;

    // Encryption wrappers
    _lib.pardox_write_prdx_encrypted = (mgr, path, pwd) => Number(_lib._raw_pardox_write_prdx_encrypted(mgr, path, pwd));

    // Contracts wrappers
    _lib.pardox_validate_contract        = (mgr, schemaJson) => Number(_lib._raw_pardox_validate_contract(mgr, schemaJson));
    _lib.pardox_contract_violation_count = () => Number(_lib._raw_pardox_contract_violation_count());
    _lib.pardox_get_quarantine_logs      = () => _readAndFreeStr(_lib._raw_pardox_get_quarantine_logs(), free);

    // Time travel wrappers
    _lib.pardox_version_write  = (mgr, path, label, ts) => Number(_lib._raw_pardox_version_write(mgr, path, label, ts));
    _lib.pardox_version_list   = (path) => _readAndFreeStr(_lib._raw_pardox_version_list(path), free);
    _lib.pardox_version_delete = (path, label) => Number(_lib._raw_pardox_version_delete(path, label));

    // Flight wrappers
    _lib.pardox_flight_start    = (port) => Number(_lib._raw_pardox_flight_start(port));
    _lib.pardox_flight_register = (name, mgr) => Number(_lib._raw_pardox_flight_register(name, mgr));

    // Cluster wrappers
    _lib.pardox_cluster_connect           = (addrsJson) => Number(_lib._raw_pardox_cluster_connect(addrsJson));
    _lib.pardox_cluster_ping              = (name) => Number(_lib._raw_pardox_cluster_ping(name));
    _lib.pardox_cluster_ping_all          = () => Number(_lib._raw_pardox_cluster_ping_all());
    _lib.pardox_cluster_checkpoint        = (path) => Number(_lib._raw_pardox_cluster_checkpoint(path));
    _lib.pardox_cluster_scatter           = (mgr, parts) => _readAndFreeStr(_lib._raw_pardox_cluster_scatter(mgr, parts), free);
    _lib.pardox_cluster_scatter_resilient = (mgr, parts, rep) => _readAndFreeStr(_lib._raw_pardox_cluster_scatter_resilient(mgr, parts, rep), free);

    // Live query wrappers
    _lib.pardox_live_query_version = (lq) => Number(_lib._raw_pardox_live_query_version(lq));

    // Misc wrappers
    _lib.pardox_legacy_ping         = (name) => Number(_lib._raw_pardox_legacy_ping(name));
    _lib.pardox_get_system_report   = () => _readAndFreeStr(_lib._raw_pardox_get_system_report(), free);
    _lib.pardox_version             = () => _lib._raw_pardox_version() || null;
    _lib.pardox_hyper_copy_v3       = (src, dst, schema, config) => Number(_lib._raw_pardox_hyper_copy_v3(src, dst, schema, config));
};
