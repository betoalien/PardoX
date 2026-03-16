'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /**
     * Scatter this DataFrame across cluster partitions.
     * @param {number} partitions  Number of partitions.
     * @returns {Object|null} Scatter metadata.
     */
    clusterScatter(partitions) {
        const lib = getLib();
        const result = lib.pardox_cluster_scatter(this._ptr, partitions);
        return result ? JSON.parse(result) : null;
    },
};
