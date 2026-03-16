'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /**
     * GroupBy aggregation.
     * @param {string|string[]} cols  Column name(s) to group by.
     * @param {Object} agg            Aggregation dict: { colName: 'sum'|'mean'|'count'|'min'|'max'|'std' }
     * @returns {DataFrame}
     */
    groupBy(cols, agg) {
        const lib = getLib();
        const colsJson = JSON.stringify(Array.isArray(cols) ? cols : [cols]);
        const aggJson = JSON.stringify(agg);
        const ptr = lib.pardox_groupby_agg(this._ptr, colsJson, aggJson);
        if (!ptr) throw new Error(`groupBy failed: cols=${cols} agg=${JSON.stringify(agg)}`);
        return this.constructor._fromPtr(ptr);
    },
};
