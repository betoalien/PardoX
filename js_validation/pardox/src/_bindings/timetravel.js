'use strict';

module.exports = function bindTimetravel(_lib) {
    _lib._raw_pardox_version_write  = _lib.raw.func('int64_t pardox_version_write(void * mgr, const char * path, const char * label, int64_t timestamp)');
    _lib.pardox_version_read        = _lib.raw.func('void * pardox_version_read(const char * path, const char * label)');
    _lib._raw_pardox_version_list   = _lib.raw.func('void * pardox_version_list(const char * path)');
    _lib._raw_pardox_version_delete = _lib.raw.func('int64_t pardox_version_delete(const char * path, const char * label)');
};
