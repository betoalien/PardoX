'use strict';

module.exports = function bindMisc(_lib) {
    _lib.pardox_reset               = _lib.raw.func('void pardox_reset()');
    _lib._raw_pardox_legacy_ping    = _lib.raw.func('int64_t pardox_legacy_ping(const char * system_name)');
    _lib._raw_pardox_get_system_report = _lib.raw.func('void * pardox_get_system_report()');
    _lib._raw_pardox_version        = _lib.raw.func('const char * pardox_version()');
    _lib._raw_pardox_hyper_copy_v3  = _lib.raw.func('int64_t pardox_hyper_copy_v3(const char * src_pattern, const char * out_path, const char * schema_json, const char * config_json)');
    _lib.pardox_ingest_buffer       = _lib.raw.func('void * pardox_ingest_buffer(void * buffer_ptr, size_t buffer_len, const char * layout_json, const char * schema_json)');
    _lib._raw_pardox_inspect_file_lazy = _lib.raw.func('const char * pardox_inspect_file_lazy(const char * path, const char * options_json)');
    _lib.pardox_import_mainframe_dat = _lib.raw.func('void * pardox_import_mainframe_dat(const char * path, const char * schema_json)');
};
