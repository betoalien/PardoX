'use strict';

module.exports = function bindEncryption(_lib) {
    _lib._raw_pardox_write_prdx_encrypted = _lib.raw.func('int pardox_write_prdx_encrypted(void * mgr, const char * path, const char * password)');
    _lib.pardox_read_prdx_encrypted       = _lib.raw.func('void * pardox_read_prdx_encrypted(const char * path, const char * password)');
};
