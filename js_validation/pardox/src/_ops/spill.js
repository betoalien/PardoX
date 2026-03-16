'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /**
     * Spill DataFrame to disk in .prdx format.
     * @param {string} path
     * @returns {number} 1 on success.
     */
    spillToDisk(path) {
        const lib = getLib();
        const result = lib.pardox_spill_to_disk(this._ptr, path);
        if (result <= 0) {
            throw new Error(`pardox_spill_to_disk failed with error code: ${result}`);
        }
        return 1;
    },

    /**
     * Load a spilled DataFrame from disk (static).
     * @param {string} path
     * @returns {DataFrame}
     */
    static_spillFromDisk(path) {
        const lib = getLib();
        const ptr = lib.pardox_spill_from_disk(path);
        if (!ptr) throw new Error('pardox_spill_from_disk returned null.');
        return this._fromPtr(ptr);
    },

    /**
     * Streaming GroupBy with chunked processing.
     */
    chunkedGroupby(groupCol, aggSpecs, chunkSize = 100000) {
        const lib = getLib();
        const aggJson = JSON.stringify(aggSpecs);
        const ptr = lib.pardox_chunked_groupby(this._ptr, groupCol, aggJson, chunkSize);
        if (!ptr) throw new Error('pardox_chunked_groupby returned null.');
        return this.constructor._fromPtr(ptr);
    },

    /**
     * External sort for large datasets.
     */
    externalSort(by, ascending = true, chunkSize = 100000) {
        const lib = getLib();
        const ascendingInt = ascending ? 1 : 0;
        const ptr = lib.pardox_external_sort(this._ptr, by, ascendingInt, chunkSize);
        if (!ptr) throw new Error('pardox_external_sort returned null.');
        return this.constructor._fromPtr(ptr);
    },

    /**
     * Get memory usage of this DataFrame in bytes (instance method).
     * @returns {number}
     */
    memoryUsage() {
        const lib = getLib();
        return lib.pardox_memory_usage(this._ptr);
    },
};
