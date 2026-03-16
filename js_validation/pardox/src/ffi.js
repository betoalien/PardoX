'use strict';

/**
 * PardoX FFI Singleton (Node.js)
 *
 * Loads the Rust shared library via koffi and registers all exported C-ABI
 * functions. Returns the same koffi library instance on every call (singleton).
 *
 * Mirrors Python's wrapper.py and PHP's Core/FFI.php.
 *
 * String memory management pattern:
 *   Functions that return heap-allocated char* are declared as returning
 *   'void *' (opaque pointer). The caller reads the string with
 *   koffi.decode(ptr, 'string') and then frees with pardox_free_string(ptr).
 *
 * 64-bit integer return values (long long / size_t):
 *   koffi returns these as BigInt. Helper functions convert to Number where safe.
 */

const koffi = require('koffi');
const { getLibraryPath } = require('./lib');
const bindAll = require('./_bindings');

/** @type {Object|null} */
let _lib = null;

/**
 * Returns the koffi library instance, loading it on first call.
 * @returns {Object} koffi lib with all function bindings.
 */
function getLib() {
    if (_lib !== null) return _lib;

    const libPath = getLibraryPath();

    let raw;
    try {
        raw = koffi.load(libPath);
    } catch (err) {
        throw new Error(`Fatal Error loading PardoX: ${err.message}`);
    }

    _lib = { raw };

    bindAll(_lib);
    _lib.pardox_init_engine();

    return _lib;
}

module.exports = { getLib };
