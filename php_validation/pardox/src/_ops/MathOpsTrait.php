<?php

namespace PardoX\DataFrameOps;

trait MathOpsTrait
{
    /**
     * Columnar addition: col_a + col_b → new DataFrame with "result_math_add".
     */
    public function add(string $colA, string $colB): self
    {
        $ptr = $this->ffi->pardox_math_add($this->ptr, $colA, $colB);
        if ($ptr === null) throw new \RuntimeException("pardox_math_add returned null.");
        return new self($ptr);
    }

    /**
     * Columnar subtraction: col_a - col_b → new DataFrame with "result_math_sub".
     */
    public function sub(string $colA, string $colB): self
    {
        $ptr = $this->ffi->pardox_math_sub($this->ptr, $colA, $colB);
        if ($ptr === null) throw new \RuntimeException("pardox_math_sub returned null.");
        return new self($ptr);
    }

    /**
     * Native arithmetic mean of a column. Returns float.
     */
    public function mean(string $col): float
    {
        return (float) $this->ffi->pardox_agg_mean($this->ptr, $col);
    }

    /**
     * Native sample standard deviation of a column. Returns float.
     */
    public function std(string $col): float
    {
        return (float) $this->ffi->pardox_math_stddev($this->ptr, $col);
    }

    /**
     * Min-Max Scaler: normalizes column values to [0, 1].
     * Returns new DataFrame with column "result_minmax".
     */
    public function min_max_scale(string $col): self
    {
        $ptr = $this->ffi->pardox_math_minmax($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("pardox_math_minmax returned null.");
        return new self($ptr);
    }

    /**
     * Sort rows by the named column.
     *
     * @param string $by        Column name to sort by.
     * @param bool   $ascending Sort order (default true = ascending).
     * @param bool   $gpu       Use GPU Bitonic sort pipeline (falls back to CPU if unavailable).
     * @return self  New sorted DataFrame.
     */
    public function sort_values(string $by, bool $ascending = true, bool $gpu = false): self
    {
        if ($gpu) {
            $ptr = $this->ffi->pardox_gpu_sort($this->ptr, $by);
        } else {
            $descending = $ascending ? 0 : 1;
            $ptr = $this->ffi->pardox_sort_values($this->ptr, $by, $descending);
        }
        if ($ptr === null) throw new \RuntimeException("Sort returned null.");
        return new self($ptr);
    }
}
