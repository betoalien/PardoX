'use strict';

const { getLib } = require('../ffi');
const { DataFrame } = require('../DataFrame');

/**
 * Start a local Arrow Flight server.
 * @param {number} port  Port number (default 8815).
 * @returns {number}
 */
function flightStart(port = 8815) {
    const lib = getLib();
    return lib.pardox_flight_start(port);
}

/**
 * Register a DataFrame on the Arrow Flight server.
 * @param {string}    name  Dataset name.
 * @param {DataFrame} df    DataFrame to register.
 * @returns {number}
 */
function flightRegister(name, df) {
    const lib = getLib();
    return lib.pardox_flight_register(name, df._ptr);
}

/**
 * Read a dataset from an Arrow Flight server.
 * @param {string} server   Server address.
 * @param {number} port     Server port.
 * @param {string} dataset  Dataset name.
 * @returns {DataFrame}
 */
function flightRead(server, port, dataset) {
    const lib = getLib();
    const ptr = lib.pardox_flight_read(server, port, dataset);
    if (!ptr) throw new Error(`flightRead failed: ${server}:${port}/${dataset}`);
    return new DataFrame(ptr);
}

/** Stop the local Arrow Flight server. */
function flightStop() {
    const lib = getLib();
    return lib.pardox_flight_stop();
}

module.exports = { flightStart, flightRegister, flightRead, flightStop };
