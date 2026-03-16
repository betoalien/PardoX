<?php

namespace PardoX\DataFrameOps;

trait MutationTrait
{
    /**
     * Cast a column to a new type in-place.
     *
     * @param string $col        Column name.
     * @param string $targetType Target type string (e.g. 'Int64', 'Float64', 'Utf8').
     */
    public function cast(string $col, string $targetType): self
    {
        if ($this->ptr === null) {
            return $this;
        }

        $res = $this->ffi->pardox_cast_column($this->ptr, $col, $targetType);

        if ($res !== 1) {
            throw new \RuntimeException("Failed to cast column '$col' to '$targetType'.");
        }

        return $this;
    }

    /**
     * Hash-join with another DataFrame.
     *
     * @param \PardoX\DataFrame $other    Right-hand DataFrame.
     * @param string|null       $on       Key column name (same on both sides).
     * @param string|null       $leftOn   Left key column (when keys differ).
     * @param string|null       $rightOn  Right key column (when keys differ).
     */
    public function join(\PardoX\DataFrame $other, ?string $on = null, ?string $leftOn = null, ?string $rightOn = null): self
    {
        $lCol = $on ?? $leftOn;
        $rCol = $on ?? $rightOn;

        if ($lCol === null || $rCol === null) {
            throw new \InvalidArgumentException("Specify 'on' or both 'leftOn' and 'rightOn'.");
        }

        $resPtr = $this->ffi->pardox_hash_join($this->ptr, $other->ptr, $lCol, $rCol);

        if ($resPtr === null) {
            throw new \RuntimeException("Join failed (Rust returned null).");
        }

        $df = new self();
        $df->ptr = $resPtr;
        return $df;
    }

    /**
     * Fill null/NaN values in all numeric columns with a scalar.
     */
    public function fillna(float $val): self
    {
        if ($this->ptr === null) {
            return $this;
        }

        foreach ($this->getDtypes() as $col => $dtype) {
            if (in_array($dtype, ['Float64', 'Int64'], true)) {
                $this->ffi->pardox_fill_na($this->ptr, $col, $val);
            }
        }

        return $this;
    }

    /**
     * Round all numeric columns to N decimal places.
     */
    public function round(int $decimals = 0): self
    {
        if ($this->ptr === null) {
            return $this;
        }

        foreach ($this->getColumns() as $col) {
            // Rust kernel ignores non-numeric columns safely
            $this->ffi->pardox_round($this->ptr, $col, $decimals);
        }

        return $this;
    }
}
