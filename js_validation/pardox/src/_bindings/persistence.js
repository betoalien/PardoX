'use strict';

module.exports = function bindPersistence(_lib) {
    _lib._raw_pardox_to_csv               = _lib.raw.func('int64_t pardox_to_csv(void * mgr, const char * path)');
    // Library exports pardox_export_to_parquet (the original ffi.js alias was pardox_to_parquet)
    _lib._raw_pardox_to_parquet           = _lib.raw.func('int64_t pardox_export_to_parquet(void * mgr, const char * path)');
    _lib._raw_pardox_write_sharded_parquet = _lib.raw.func('int64_t pardox_write_sharded_parquet(void * mgr, const char * directory, int64_t max_rows_per_shard)');
};
