<?php

namespace PardoX;

use PardoX\Core\FFI;
use RuntimeException;
use FFI\CData;

/**
 * PardoX Series — a single column view into a DataFrame.
 *
 * Arithmetic, comparison, and aggregation operations delegate to the Rust
 * engine via FFI. The Series does NOT own the memory; the parent DataFrame
 * manages the HyperBlockManager lifetime.
 *
 * Op-code mapping for comparisons (mirrors series.py):
 *   0 = Eq, 1 = Neq, 2 = Gt, 3 = Gte, 4 = Lt, 5 = Lte
 */
class Series
{
    /** @var \FFI */
    private $ffi;

    /** @var DataFrame  Parent DataFrame that owns the Rust memory. */
    private DataFrame $df;

    /** @var string  Column name within the parent DataFrame. */
    public string $name;

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    /**
     * @param DataFrame $df    Parent DataFrame.
     * @param string    $name  Column name this Series represents.
     */
    public function __construct(DataFrame $df, string $name)
    {
        $this->ffi  = FFI::getInstance();
        $this->df   = $df;
        $this->name = $name;
    }

    // No __destruct — memory is owned by the parent DataFrame.

    // -------------------------------------------------------------------------
    // Magic properties
    // -------------------------------------------------------------------------

    public function __get(string $prop)
    {
        if ($prop === 'dtype') {
            return $this->df->dtypes[$this->name] ?? 'Unknown';
        }
        throw new \BadMethodCallException("Undefined property: Series::\$$prop");
    }

    public function __toString(): string
    {
        return $this->name;
    }

    // -------------------------------------------------------------------------
    // Inspection & Visualization
    // -------------------------------------------------------------------------

    /** Return a new Series containing the first N rows. */
    public function head(int $n = 5): self
    {
        $sliced = $this->df->head($n);
        return new self($sliced, $this->name);
    }

    /** Return a new Series containing the last N rows. */
    public function tail(int $n = 5): self
    {
        $sliced = $this->df->tail($n);
        return new self($sliced, $this->name);
    }

    /** Print the parent DataFrame's ASCII table (mirrors Python's show()). */
    public function show(int $n = 10): void
    {
        $this->df->show($n);
    }

    // -------------------------------------------------------------------------
    // Arithmetic Operations
    // -------------------------------------------------------------------------

    public function add(Series $other): self
    {
        return $this->arithmeticOp('pardox_series_add', $other);
    }

    public function sub(Series $other): self
    {
        return $this->arithmeticOp('pardox_series_sub', $other);
    }

    public function mul(Series $other): self
    {
        return $this->arithmeticOp('pardox_series_mul', $other);
    }

    public function div(Series $other): self
    {
        return $this->arithmeticOp('pardox_series_div', $other);
    }

    public function mod(Series $other): self
    {
        return $this->arithmeticOp('pardox_series_mod', $other);
    }

    // -------------------------------------------------------------------------
    // Comparison Operations (return Boolean Series usable as filter mask)
    // -------------------------------------------------------------------------

    /** Equal (op_code = 0) */
    public function eq($other): self
    {
        return $this->cmpOp($other, 0);
    }

    /** Not equal (op_code = 1) */
    public function neq($other): self
    {
        return $this->cmpOp($other, 1);
    }

    /** Greater than (op_code = 2) */
    public function gt($other): self
    {
        return $this->cmpOp($other, 2);
    }

    /** Greater than or equal (op_code = 3) */
    public function gte($other): self
    {
        return $this->cmpOp($other, 3);
    }

    /** Less than (op_code = 4) */
    public function lt($other): self
    {
        return $this->cmpOp($other, 4);
    }

    /** Less than or equal (op_code = 5) */
    public function lte($other): self
    {
        return $this->cmpOp($other, 5);
    }

    // -------------------------------------------------------------------------
    // Aggregations
    // -------------------------------------------------------------------------

    public function sum(): float
    {
        return (float) $this->ffi->pardox_agg_sum($this->df->_ptr(), $this->name);
    }

    public function mean(): float
    {
        return (float) $this->ffi->pardox_agg_mean($this->df->_ptr(), $this->name);
    }

    public function min(): float
    {
        return (float) $this->ffi->pardox_agg_min($this->df->_ptr(), $this->name);
    }

    public function max(): float
    {
        return (float) $this->ffi->pardox_agg_max($this->df->_ptr(), $this->name);
    }

    public function count(): int
    {
        return (int) $this->ffi->pardox_agg_count($this->df->_ptr(), $this->name);
    }

    public function std(): float
    {
        return (float) $this->ffi->pardox_agg_std($this->df->_ptr(), $this->name);
    }

    // -------------------------------------------------------------------------
    // Cleaning (in-place, mirrors DataFrame methods)
    // -------------------------------------------------------------------------

    public function fillna(float $val): self
    {
        $res = $this->ffi->pardox_fill_na($this->df->_ptr(), $this->name, $val);
        if ($res < 0) {
            throw new RuntimeException("fillna failed on column '{$this->name}'.");
        }
        return $this;
    }

    public function round(int $decimals): self
    {
        $res = $this->ffi->pardox_round($this->df->_ptr(), $this->name, $decimals);
        if ($res < 0) {
            throw new RuntimeException("round failed on column '{$this->name}' (must be numeric).");
        }
        return $this;
    }

    // -------------------------------------------------------------------------
    // Internal helpers (used by DataFrame::filter and arithmetic chaining)
    // -------------------------------------------------------------------------

    /**
     * Expose the raw Rust pointer of the parent DataFrame for FFI calls.
     * @internal
     */
    public function _dfPtr(): ?CData
    {
        return $this->df->_ptr();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private function arithmeticOp(string $func, Series $other): self
    {
        $this->validateRowCount($other);

        $resPtr = $this->ffi->$func(
            $this->df->_ptr(),
            $this->name,
            $other->df->_ptr(),
            $other->name
        );

        return $this->wrapResult($resPtr);
    }

    private function cmpOp($other, int $opCode): self
    {
        if ($other instanceof Series) {
            $this->validateRowCount($other);

            $resPtr = $this->ffi->pardox_filter_compare(
                $this->df->_ptr(),
                $this->name,
                $other->df->_ptr(),
                $other->name,
                $opCode
            );

            return $this->wrapResult($resPtr);
        }

        if (is_int($other) || is_float($other)) {
            $isFloat = is_float($other) ? 1 : 0;
            $valF64  = (float) $other;
            $valI64  = (int)   $other;

            $resPtr = $this->ffi->pardox_filter_compare_scalar(
                $this->df->_ptr(),
                $this->name,
                $valF64,
                $valI64,
                $isFloat,
                $opCode
            );

            return $this->wrapResult($resPtr);
        }

        throw new \InvalidArgumentException(
            "Comparison operand must be a Series or numeric scalar. Got: " . gettype($other)
        );
    }

    private function validateRowCount(Series $other): void
    {
        $leftRows  = $this->df->shape[0];
        $rightRows = $other->df->shape[0];

        if ($leftRows !== $rightRows) {
            throw new \LengthException(
                "Length mismatch: left has $leftRows rows, right has $rightRows rows."
            );
        }
    }

    private function wrapResult(?CData $resPtr): self
    {
        if ($resPtr === null) {
            throw new RuntimeException("Operation failed (Rust returned null pointer).");
        }

        $resultDf = new DataFrame($resPtr);
        $columns  = $resultDf->columns;

        if (empty($columns)) {
            throw new RuntimeException("Compute engine returned empty result schema.");
        }

        // Rust generates a name like 'result_add' — take the first column
        return new self($resultDf, $columns[0]);
    }
}
