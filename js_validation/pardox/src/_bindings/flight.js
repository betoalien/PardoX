'use strict';

module.exports = function bindFlight(_lib) {
    _lib._raw_pardox_flight_start    = _lib.raw.func('int64_t pardox_flight_start(int port)');
    _lib._raw_pardox_flight_register = _lib.raw.func('int64_t pardox_flight_register(const char * name, void * mgr)');
    _lib.pardox_flight_stop          = _lib.raw.func('int pardox_flight_stop()');
    _lib.pardox_flight_read          = _lib.raw.func('void * pardox_flight_read(const char * server, uint16_t port, const char * dataset)');
};
