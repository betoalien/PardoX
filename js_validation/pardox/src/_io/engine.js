'use strict';

const { getLib } = require('../ffi');

/** Reset the PardoX global engine state. */
function reset() {
    const lib = getLib();
    lib.pardox_reset();
}

/**
 * Ping the PardoX engine.
 * @param {string} systemName  System identifier.
 * @returns {number}
 */
function ping(systemName = 'pardox') {
    const lib = getLib();
    return lib.pardox_legacy_ping(systemName);
}

/**
 * Return the PardoX Core library version string.
 * @returns {string|null}
 */
function engineVersion() {
    const lib = getLib();
    return lib.pardox_version();
}

/**
 * Return a system report from the engine.
 * @returns {Object}
 */
function systemReport() {
    const lib    = getLib();
    const result = lib.pardox_get_system_report();
    return result ? JSON.parse(result) : {};
}

/** Return quarantine log entries. */
function getQuarantineLogs() {
    const lib    = getLib();
    const result = lib.pardox_get_quarantine_logs();
    return result ? JSON.parse(result) : [];
}

/** Clear all quarantine logs. */
function clearQuarantine() {
    const lib = getLib();
    lib.pardox_clear_quarantine();
}

module.exports = { reset, ping, engineVersion, systemReport, getQuarantineLogs, clearQuarantine };
