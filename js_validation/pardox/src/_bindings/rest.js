'use strict';

module.exports = function bindRest(_lib) {
    _lib.pardox_read_rest = _lib.raw.func('void * pardox_read_rest(const char * url, const char * method, const char * headers_json)');
};
