<?php

namespace PardoX\DataFrameOps;

trait SelectionTrait
{
    /**
     * Apply a boolean Series as a row filter, returning a new DataFrame.
     */
    public function filter(\PardoX\Series $mask): self
    {
        if (strpos($mask->dtype, 'Boolean') === false) {
            throw new \InvalidArgumentException("Filter mask must be a Boolean Series. Got: {$mask->dtype}");
        }

        $resPtr = $this->ffi->pardox_apply_filter($this->ptr, $mask->_dfPtr(), $mask->name);

        if ($resPtr === null) {
            throw new \RuntimeException("Filter operation returned null.");
        }

        $df = new self();
        $df->ptr = $resPtr;
        return $df;
    }

    /**
     * Return a slice (new DataFrame) by row positions.
     */
    public function iloc(int $start, int $end): self
    {
        if ($this->ptr === null) {
            return $this;
        }
        return $this->slice($start, $end - $start);
    }

    // -------------------------------------------------------------------------
    // ArrayAccess — $df['col'] and $df['col'] = $series
    // -------------------------------------------------------------------------

    public function offsetExists($offset): bool
    {
        return in_array($offset, $this->getColumns(), true);
    }

    /** @return \PardoX\Series */
    public function offsetGet($offset): \PardoX\Series
    {
        if (!is_string($offset)) {
            throw new \InvalidArgumentException("Column key must be a string.");
        }
        return new \PardoX\Series($this, $offset);
    }

    /** Assign a computed Series back as a new (or replaced) column. */
    public function offsetSet($offset, $value): void
    {
        if (!is_string($offset)) {
            throw new \InvalidArgumentException("Column key must be a string.");
        }

        if (!($value instanceof \PardoX\Series)) {
            throw new \InvalidArgumentException("Value must be a PardoX Series.");
        }

        $res = $this->ffi->pardox_add_column($this->ptr, $value->_dfPtr(), $offset);

        if ($res !== 1) {
            $errors = [
                -1 => 'Invalid Pointers',
                -2 => 'Invalid Column Name',
                -3 => 'Engine Logic Error (row mismatch or duplicate name)',
            ];
            $msg = $errors[(int)$res] ?? "Unknown error: $res";
            throw new \RuntimeException("Column assignment failed: $msg");
        }
    }

    public function offsetUnset($offset): void
    {
        throw new \BadMethodCallException("Column deletion is not supported.");
    }
}
