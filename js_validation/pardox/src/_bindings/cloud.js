'use strict';

module.exports = function bindCloud(_lib) {
    _lib.pardox_cloud_read_csv   = _lib.raw.func('void * pardox_cloud_read_csv(const char * url, const char * schema, const char * config, const char * credentials)');
    _lib.pardox_cloud_read_prdx  = _lib.raw.func('void * pardox_cloud_read_prdx(const char * url, int64_t limit, const char * credentials)');
    _lib._raw_pardox_cloud_write_prdx = _lib.raw.func('int64_t pardox_cloud_write_prdx(void * mgr, const char * url, const char * credentials)');
};
