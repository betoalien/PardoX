'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /** Serialize up to `limit` rows as a JSON string (preview). */
    toJson(limit = 100) {
        const lib = getLib();
        // If called with no arguments, export all rows (full export)
        if (arguments.length === 0) {
            const json = lib.pardox_to_json_records(this._ptr);
            return json || '[]';
        }
        return lib.pardox_manager_to_json(this._ptr, limit) || '[]';
    },

    /**
     * Returns ALL rows as an array of objects (records format).
     * @returns {Object[]}
     */
    toDict() {
        const lib = getLib();
        const json = lib.pardox_to_json_records(this._ptr);
        if (!json) return [];
        try { return JSON.parse(json); } catch { return []; }
    },

    /**
     * Returns ALL rows as an array of arrays (values format).
     * @returns {Array[]}
     */
    toList() {
        const lib = getLib();
        const json = lib.pardox_to_json_arrays(this._ptr);
        if (!json) return [];
        try { return JSON.parse(json); } catch { return []; }
    },

    /**
     * Returns the frequency of each unique value in the given column.
     * @param {string} col
     * @returns {Object}
     */
    valueCounts(col) {
        const lib = getLib();
        const json = lib.pardox_value_counts(this._ptr, col);
        if (!json) return {};
        try { return JSON.parse(json); } catch { return {}; }
    },

    /**
     * Returns the unique values of the given column.
     * @param {string} col
     * @returns {Array}
     */
    unique(col) {
        const lib = getLib();
        const json = lib.pardox_unique(this._ptr, col);
        if (!json) return [];
        try { return JSON.parse(json); } catch { return []; }
    },
};
