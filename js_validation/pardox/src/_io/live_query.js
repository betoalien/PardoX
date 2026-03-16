'use strict';

const { getLib } = require('../ffi');
const { DataFrame } = require('../DataFrame');

/**
 * Start a live query that auto-refreshes when the .prdx source changes.
 * @param {string} prdxPath  Path to the .prdx file to watch.
 * @param {string} sqlQuery  SQL query to run on each refresh.
 * @param {number} pollMs    Polling interval in milliseconds.
 * @returns {*} Opaque LiveQuery handle.
 */
function liveQueryStart(prdxPath, sqlQuery, pollMs = 1000) {
    const lib = getLib();
    const lq  = lib.pardox_live_query_start(prdxPath, sqlQuery, pollMs);
    if (!lq) throw new Error(`liveQueryStart failed for: ${prdxPath}`);
    return lq;
}

/**
 * Get the current version counter of a live query.
 * @param {*} lq  LiveQuery handle.
 * @returns {number}
 */
function liveQueryVersion(lq) {
    const lib = getLib();
    return lib.pardox_live_query_version(lq);
}

/**
 * Take the latest result from a live query as a DataFrame.
 * @param {*} lq  LiveQuery handle.
 * @returns {DataFrame|null}
 */
function liveQueryTake(lq) {
    const lib = getLib();
    const ptr = lib.pardox_live_query_take(lq);
    if (!ptr) return null;
    return new DataFrame(ptr);
}

/**
 * Free a live query handle.
 * @param {*} lq  LiveQuery handle.
 */
function liveQueryFree(lq) {
    const lib = getLib();
    lib.pardox_live_query_free(lq);
}

module.exports = { liveQueryStart, liveQueryVersion, liveQueryTake, liveQueryFree };
