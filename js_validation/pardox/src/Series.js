'use strict';

/**
 * PardoX Series for Node.js
 *
 * Represents a single column view into a DataFrame. Arithmetic, comparison,
 * and aggregation operations delegate to the Rust engine via koffi FFI.
 *
 * The Series does NOT own the Rust memory; the parent DataFrame manages
 * the HyperBlockManager pointer lifecycle.
 *
 * Comparison op-codes (mirrors Python series.py):
 *   0 = Eq, 1 = Neq, 2 = Gt, 3 = Gte, 4 = Lt, 5 = Lte
 *
 * Design note:
 *   All FFI calls are strictly synchronous to avoid calling-convention issues
 *   between Node.js/koffi and the Rust engine.
 */

const { getLib } = require('./ffi');
const { PTR_SYMBOL } = require('./DataFrame');

class Series {
    /**
     * @param {import('./DataFrame').DataFrame} df    Parent DataFrame.
     * @param {string}                          name  Column name.
     */
    constructor(df, name) {
        this._df   = df;
        this.name  = name;
    }

    // Series does NOT call pardox_free_manager — memory is owned by parent DataFrame.

    // -------------------------------------------------------------------------
    // Properties
    // -------------------------------------------------------------------------

    get dtype() {
        return this._df.dtypes[this.name] || 'Unknown';
    }

    toString() {
        return this.name;
    }

    // -------------------------------------------------------------------------
    // Inspection & Visualization
    // -------------------------------------------------------------------------

    /** Return a new Series containing the first N rows. */
    head(n = 5) {
        const sliced = this._df.head(n);
        return new Series(sliced, this.name);
    }

    /** Return a new Series containing the last N rows. */
    tail(n = 5) {
        const sliced = this._df.tail(n);
        return new Series(sliced, this.name);
    }

    /** Print the parent DataFrame's ASCII table. */
    show(n = 10) {
        this._df.show(n);
    }

    // -------------------------------------------------------------------------
    // Arithmetic Operations
    // -------------------------------------------------------------------------

    add(other) { return this._arith('pardox_series_add', other); }
    sub(other) { return this._arith('pardox_series_sub', other); }
    mul(other) { return this._arith('pardox_series_mul', other); }
    div(other) { return this._arith('pardox_series_div', other); }
    mod(other) { return this._arith('pardox_series_mod', other); }

    // -------------------------------------------------------------------------
    // Comparison Operations (return Boolean Series usable as filter mask)
    // -------------------------------------------------------------------------

    /** Equal        (op_code = 0) */ eq(other)  { return this._cmp(other, 0); }
    /** Not equal    (op_code = 1) */ neq(other) { return this._cmp(other, 1); }
    /** Greater than (op_code = 2) */ gt(other)  { return this._cmp(other, 2); }
    /** Greater ≥    (op_code = 3) */ gte(other) { return this._cmp(other, 3); }
    /** Less than    (op_code = 4) */ lt(other)  { return this._cmp(other, 4); }
    /** Less ≤       (op_code = 5) */ lte(other) { return this._cmp(other, 5); }

    // -------------------------------------------------------------------------
    // Aggregations
    // -------------------------------------------------------------------------

    sum()   { return getLib().pardox_agg_sum(this._df[PTR_SYMBOL],   this.name); }
    mean()  { return getLib().pardox_agg_mean(this._df[PTR_SYMBOL],  this.name); }
    min()   { return getLib().pardox_agg_min(this._df[PTR_SYMBOL],   this.name); }
    max()   { return getLib().pardox_agg_max(this._df[PTR_SYMBOL],   this.name); }
    count() { return Math.round(getLib().pardox_agg_count(this._df[PTR_SYMBOL], this.name)); }
    std()   { return getLib().pardox_agg_std(this._df[PTR_SYMBOL],   this.name); }

    // -------------------------------------------------------------------------
    // Cleaning
    // -------------------------------------------------------------------------

    fillna(val) {
        const res = getLib().pardox_fill_na(this._df[PTR_SYMBOL], this.name, val);
        if (res < 0) throw new Error(`fillna failed on column '${this.name}'.`);
        return this;
    }

    round(decimals) {
        const res = getLib().pardox_round(this._df[PTR_SYMBOL], this.name, decimals);
        if (res < 0) throw new Error(`round failed on column '${this.name}' (must be numeric).`);
        return this;
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /** Expose the raw Rust pointer of the parent DataFrame for FFI calls. */
    _parentPtr() {
        return this._df[PTR_SYMBOL];
    }

    _validateLength(other) {
        const leftRows  = this._df.shape[0];
        const rightRows = other._df.shape[0];
        if (leftRows !== rightRows) {
            throw new Error(`Length mismatch: left has ${leftRows} rows, right has ${rightRows} rows.`);
        }
    }

    _arith(funcName, other) {
        if (!(other instanceof Series)) {
            throw new TypeError(`Arithmetic operand must be a PardoX Series. Got: ${typeof other}`);
        }
        this._validateLength(other);

        const lib    = getLib();
        const resPtr = lib[funcName](
            this._df[PTR_SYMBOL], this.name,
            other._df[PTR_SYMBOL], other.name
        );

        return this._wrapResult(resPtr);
    }

    _cmp(other, opCode) {
        const lib = getLib();

        if (other instanceof Series) {
            this._validateLength(other);
            const resPtr = lib.pardox_filter_compare(
                this._df[PTR_SYMBOL], this.name,
                other._df[PTR_SYMBOL], other.name,
                opCode
            );
            return this._wrapResult(resPtr);
        }

        if (typeof other === 'number') {
            const isFloat  = Number.isInteger(other) ? 0 : 1;
            const valF64   = other;
            const valI64   = Math.trunc(other);

            const resPtr = lib.pardox_filter_compare_scalar(
                this._df[PTR_SYMBOL], this.name,
                valF64, BigInt(valI64), isFloat,
                opCode
            );
            return this._wrapResult(resPtr);
        }

        throw new TypeError(
            `Comparison operand must be a Series or number. Got: ${typeof other}`
        );
    }

    _wrapResult(resPtr) {
        const { DataFrame } = require('./DataFrame');

        if (!resPtr) {
            throw new Error('Operation failed (Rust returned null pointer).');
        }

        const resultDf  = new DataFrame(resPtr);
        const cols      = resultDf.columns;

        if (cols.length === 0) {
            throw new Error('Compute engine returned empty result schema.');
        }

        // Rust generates a name like 'result_add' — take the first column
        return new Series(resultDf, cols[0]);
    }
}

module.exports = { Series };
