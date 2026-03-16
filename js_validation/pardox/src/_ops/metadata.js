'use strict';

const { getLib } = require('../ffi');

module.exports = {
    get shape() {
        const lib = getLib();
        const rows = lib.pardox_get_row_count(this._ptr);
        const cols = this.columns.length;
        return [rows, cols];
    },

    get columns() {
        const schema = this._schema();
        return (schema.columns || []).map(c => c.name);
    },

    get dtypes() {
        const schema = this._schema();
        const result = {};
        for (const col of (schema.columns || [])) {
            result[col.name] = col.type;
        }
        return result;
    },
};
