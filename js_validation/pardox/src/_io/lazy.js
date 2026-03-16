'use strict';

const { getLib } = require('../ffi');
const { DataFrame } = require('../DataFrame');

/**
 * Create a LazyFrame by scanning a CSV file (deferred execution).
 * @param {string}  path       Path to the CSV file.
 * @param {number}  delimiter  ASCII code for delimiter (default 44 = comma).
 * @param {boolean} hasHeader  Whether the file has a header row.
 * @returns {*} Opaque LazyFrame pointer.
 */
function scanCsv(path, delimiter = 44, hasHeader = true) {
    const lib = getLib();
    const ptr = lib.pardox_lazy_scan_csv(path, delimiter, hasHeader ? 1 : 0);
    if (!ptr) throw new Error(`scanCsv failed for: ${path}`);
    return ptr;
}

/**
 * Create a LazyFrame from a .prdx file (deferred execution).
 * @param {string} path  Path to the .prdx file.
 * @returns {*} Opaque LazyFrame pointer.
 */
function scanPrdx(path) {
    const lib = getLib();
    const ptr = lib.pardox_lazy_scan_prdx(path);
    if (!ptr) throw new Error(`scanPrdx failed for: ${path}`);
    return ptr;
}

/**
 * Apply query planner optimizations to a LazyFrame.
 * @param {*} lazyFrame  LazyFrame pointer.
 * @returns {*} Optimized LazyFrame pointer.
 */
function lazyOptimize(lazyFrame) {
    const lib = getLib();
    return lib.pardox_lazy_optimize(lazyFrame);
}

/**
 * Return statistics about a lazy plan.
 * @param {*} lazyFrame  LazyFrame pointer.
 * @returns {Object|null}
 */
function lazyStats(lazyFrame) {
    const lib    = getLib();
    const result = lib.pardox_lazy_stats(lazyFrame);
    return result ? JSON.parse(result) : null;
}

module.exports = { scanCsv, scanPrdx, lazyOptimize, lazyStats };
