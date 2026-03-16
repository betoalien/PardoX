'use strict';

module.exports = function bindLiveQuery(_lib) {
    _lib.pardox_live_query_start    = _lib.raw.func('void * pardox_live_query_start(const char * prdx_path, const char * sql_query, int poll_ms)');
    _lib._raw_pardox_live_query_version = _lib.raw.func('int pardox_live_query_version(void * lq)');
    _lib.pardox_live_query_take     = _lib.raw.func('void * pardox_live_query_take(void * lq)');
    _lib.pardox_live_query_free     = _lib.raw.func('void pardox_live_query_free(void * lq)');
};
