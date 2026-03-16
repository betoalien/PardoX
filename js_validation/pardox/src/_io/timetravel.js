'use strict';

const { getLib } = require('../ffi');
const { DataFrame } = require('../DataFrame');

/**
 * Read a versioned snapshot from disk.
 * @param {string} path   Base directory path.
 * @param {string} label  Version label.
 * @returns {DataFrame}
 */
function versionRead(path, label) {
    const lib = getLib();
    const ptr = lib.pardox_version_read(path, label);
    if (!ptr) throw new Error(`versionRead failed: path=${path}, label=${label}`);
    return new DataFrame(ptr);
}

/**
 * List all version labels for a base path.
 * @param {string} path
 * @returns {string[]}
 */
function versionList(path) {
    const lib    = getLib();
    const result = lib.pardox_version_list(path);
    return result ? JSON.parse(result) : [];
}

/**
 * Delete a specific version snapshot.
 * @param {string} path
 * @param {string} label
 * @returns {number}
 */
function versionDelete(path, label) {
    const lib = getLib();
    return lib.pardox_version_delete(path, label);
}

module.exports = { versionRead, versionList, versionDelete };
