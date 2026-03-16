'use strict';

module.exports = function bindDatabases(_lib) {
    // MySQL
    _lib.pardox_read_mysql        = _lib.raw.func('void * pardox_read_mysql(const char * conn_str, const char * query)');
    _lib._raw_pardox_write_mysql  = _lib.raw.func('int64_t pardox_write_mysql(void * mgr, const char * conn_str, const char * table, const char * mode, const char * conflict_cols_json)');
    _lib._raw_pardox_execute_mysql = _lib.raw.func('int64_t pardox_execute_mysql(const char * conn_str, const char * query)');

    // SQL Server
    _lib.pardox_read_sqlserver        = _lib.raw.func('void * pardox_read_sqlserver(const char * conn_str, const char * query)');
    _lib._raw_pardox_write_sqlserver  = _lib.raw.func('int64_t pardox_write_sqlserver(void * mgr, const char * conn_str, const char * table, const char * mode, const char * conflict_cols_json)');
    _lib._raw_pardox_execute_sqlserver = _lib.raw.func('int64_t pardox_execute_sqlserver(const char * conn_str, const char * query)');

    // MongoDB
    _lib.pardox_read_mongodb        = _lib.raw.func('void * pardox_read_mongodb(const char * conn_str, const char * db_dot_collection)');
    _lib._raw_pardox_write_mongodb  = _lib.raw.func('int64_t pardox_write_mongodb(void * mgr, const char * conn_str, const char * db_dot_collection, const char * mode)');
    _lib._raw_pardox_execute_mongodb = _lib.raw.func('int64_t pardox_execute_mongodb(const char * conn_str, const char * database, const char * command_json)');
};
