<?php

namespace PardoX\DataFrameOps;

trait NestedTrait
{
    /**
     * Extract a value from a JSON string column using dot-notation path.
     *
     * @param string $col      Column containing JSON strings.
     * @param string $keyPath  Dot-notation path to extract (e.g., "address.city").
     * @return self New DataFrame with extracted values as strings.
     */
    public function json_extract(string $col, string $keyPath): self
    {
        $ptr = $this->ffi->pardox_json_extract($this->ptr, $col, $keyPath);
        if ($ptr === null) throw new \RuntimeException("pardox_json_extract returned null.");
        return new self($ptr);
    }

    /**
     * Explode a JSON array column, creating one row per array element.
     *
     * @param string $col Column containing JSON array strings.
     * @return self New DataFrame with exploded rows (original row repeated for each element).
     */
    public function explode(string $col): self
    {
        $ptr = $this->ffi->pardox_explode($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("pardox_explode returned null.");
        return new self($ptr);
    }

    /**
     * Unnest a JSON object column into multiple columns.
     *
     * @param string $col Column containing JSON object strings.
     * @return self New DataFrame with flattened columns (original column replaced with expanded fields).
     */
    public function unnest(string $col): self
    {
        $ptr = $this->ffi->pardox_unnest($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("pardox_unnest returned null.");
        return new self($ptr);
    }
}
