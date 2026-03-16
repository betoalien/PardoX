'use strict';

module.exports = function bindSpill(_lib) {
    _lib._raw_pardox_spill_to_disk   = _lib.raw.func('int64_t pardox_spill_to_disk(void * mgr, const char * path)');
    _lib.pardox_spill_from_disk      = _lib.raw.func('void * pardox_spill_from_disk(const char * path)');
    _lib.pardox_external_sort        = _lib.raw.func('void * pardox_external_sort(void * mgr, const char * col, int32_t ascending, int64_t chunk_size)');
    _lib._raw_pardox_memory_usage    = _lib.raw.func('int64_t pardox_memory_usage(void * mgr)');
};
