'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /**
     * Execute a SQL SELECT query on this DataFrame.
     * @param {string} sqlQuery
     * @returns {DataFrame|null}
     */
    sql(sqlQuery) {
        const lib = getLib();
        const ptr = lib.pardox_sql_query(this._ptr, sqlQuery);
        if (!ptr) return null;
        return this.constructor._fromPtr(ptr);
    },
};
