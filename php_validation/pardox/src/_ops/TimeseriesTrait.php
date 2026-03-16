<?php

namespace PardoX\DataFrameOps;

trait TimeseriesTrait
{
    /**
     * Forward fill NaN values in a column (propagate last valid value forward).
     *
     * @param string $col Column name to fill.
     * @return self New DataFrame with a single column containing filled values.
     */
    public function ffill(string $col): self
    {
        $ptr = $this->ffi->pardox_ffill($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("pardox_ffill returned null.");
        return new self($ptr);
    }

    /**
     * Backward fill NaN values in a column (propagate next valid value backward).
     *
     * @param string $col Column name to fill.
     * @return self New DataFrame with a single column containing filled values.
     */
    public function bfill(string $col): self
    {
        $ptr = $this->ffi->pardox_bfill($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("pardox_bfill returned null.");
        return new self($ptr);
    }

    /**
     * Linear interpolation of NaN values in a column.
     *
     * @param string $col Column name to interpolate.
     * @return self New DataFrame with a single column containing interpolated values.
     */
    public function interpolate(string $col): self
    {
        $ptr = $this->ffi->pardox_interpolate($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("pardox_interpolate returned null.");
        return new self($ptr);
    }
}
