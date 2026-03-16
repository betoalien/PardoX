'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /**
     * Pivot table transformation.
     */
    pivotTable(index, columns, values, aggFunc = 'sum') {
        const lib = getLib();
        const ptr = lib.pardox_pivot_table(this._ptr, index, columns, values, aggFunc);
        if (!ptr) throw new Error('pardox_pivot_table returned null. Check column names and agg_func.');
        return this.constructor._fromPtr(ptr);
    },

    /**
     * Melt (unpivot) transformation.
     */
    melt(idVars, valueVars, varName = 'variable', valueName = 'value') {
        const lib = getLib();
        const idVarsJson = JSON.stringify(Array.isArray(idVars) ? idVars : [idVars]);
        const valueVarsJson = JSON.stringify(Array.isArray(valueVars) ? valueVars : [valueVars]);
        const ptr = lib.pardox_melt(this._ptr, idVarsJson, valueVarsJson, varName, valueName);
        if (!ptr) throw new Error('pardox_melt returned null. Check column names.');
        return this.constructor._fromPtr(ptr);
    },
};
