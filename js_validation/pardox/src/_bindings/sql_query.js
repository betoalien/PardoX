'use strict';

module.exports = function bindSqlQuery(_lib) {
    _lib.pardox_sql_query         = _lib.raw.func('void * pardox_sql_query(void * mgr, const char * sql)');
    _lib._raw_pardox_execute_sql  = _lib.raw.func('int64_t pardox_execute_sql(const char * conn_str, const char * query)');
    _lib._raw_pardox_write_sql    = _lib.raw.func('int64_t pardox_write_sql(void * mgr, const char * conn_str, const char * table, const char * mode, const char * conflict_cols_json)');
    _lib.pardox_sqlserver_config_ok = _lib.raw.func('int pardox_sqlserver_config_ok()');
};
