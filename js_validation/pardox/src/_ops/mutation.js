'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /** Cast a column to a new type in-place. Returns `this` for chaining. */
    cast(col, targetType) {
        const lib = getLib();
        const res = lib.pardox_cast_column(this._ptr, col, targetType);
        if (res !== 1) {
            throw new Error(`Failed to cast column '${col}' to '${targetType}'.`);
        }
        return this;
    },

    /**
     * Hash-join with another DataFrame.
     * @param {DataFrame}   other
     * @param {string|null} on        Key column (same name on both sides).
     * @param {string|null} leftOn    Left key column.
     * @param {string|null} rightOn   Right key column.
     */
    join(other, { on = null, leftOn = null, rightOn = null } = {}) {
        const lCol = on || leftOn;
        const rCol = on || rightOn;
        if (!lCol || !rCol) {
            throw new Error("Specify 'on' or both 'leftOn' and 'rightOn'.");
        }
        const lib = getLib();
        const resPtr = lib.pardox_hash_join(this._ptr, other._ptr, lCol, rCol);
        if (!resPtr) throw new Error('Join failed (Rust returned null).');
        return this.constructor._fromPtr(resPtr);
    },

    /** Fill null/NaN in all numeric columns. Returns `this` for chaining. */
    fillna(val) {
        const lib = getLib();
        for (const [col, dtype] of Object.entries(this.dtypes)) {
            if (['Float64', 'Int64'].includes(dtype)) {
                lib.pardox_fill_na(this._ptr, col, val);
            }
        }
        return this;
    },

    /** Round all numeric columns to N decimal places. Returns `this` for chaining. */
    round(decimals = 0) {
        const lib = getLib();
        for (const col of this.columns) {
            lib.pardox_round(this._ptr, col, decimals);
        }
        return this;
    },

    /**
     * Add a column from a Series (or internal use).
     * @param {string} colName
     * @param {Series} series
     */
    addColumn(colName, series) {
        this._setColumn(colName, series);
        return this;
    },
};
