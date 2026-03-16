'use strict';

const { getLib } = require('../ffi');

/**
 * Define metadata getters on DataFrame.prototype using Object.defineProperty.
 * Must NOT be passed to Object.assign() — getters in a plain object literal
 * are invoked immediately during assignment, not lazily.
 *
 * @param {Object} proto  DataFrame.prototype
 */
function defineMetadata(proto) {
    Object.defineProperty(proto, 'shape', {
        get() {
            const lib  = getLib();
            const rows = lib.pardox_get_row_count(this._ptr);
            const cols = this.columns.length;
            return [rows, cols];
        },
        enumerable:   true,
        configurable: true,
    });

    Object.defineProperty(proto, 'columns', {
        get() {
            const schema = this._schema();
            return (schema.columns || []).map(c => c.name);
        },
        enumerable:   true,
        configurable: true,
    });

    Object.defineProperty(proto, 'dtypes', {
        get() {
            const schema = this._schema();
            const result = {};
            for (const col of (schema.columns || [])) {
                result[col.name] = col.type;
            }
            return result;
        },
        enumerable:   true,
        configurable: true,
    });
}

module.exports = { defineMetadata };
