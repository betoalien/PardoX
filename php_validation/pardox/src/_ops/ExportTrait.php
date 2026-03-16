<?php

namespace PardoX\DataFrameOps;

trait ExportTrait
{
    /**
     * Serialize ALL rows to a JSON string (records format, no row limit).
     * Unlike the preview helpers (show/head), this exports the full DataFrame.
     *
     * @return string  JSON array string "[{...}, ...]".
     */
    public function to_json(): string
    {
        if ($this->ptr === null) {
            return '[]';
        }

        $jsonC = $this->ffi->pardox_to_json_records($this->ptr);
        if ($jsonC === null) {
            return '[]';
        }

        $json = \FFI::string($jsonC);
        $this->ffi->pardox_free_string($jsonC);
        return $json;
    }

    /**
     * Returns ALL rows as an array of associative arrays (records format).
     * Equivalent to Pandas' df.to_dict('records').
     *
     * @return array  [['col' => val, ...], ...]
     */
    public function to_dict(): array
    {
        $json = $this->to_json();
        return json_decode($json, true) ?? [];
    }

    /**
     * Returns ALL rows as an array of arrays (values format).
     * Equivalent to Pandas' df.values.tolist().
     *
     * @return array  [[val, val, ...], ...]
     */
    public function tolist(): array
    {
        if ($this->ptr === null) {
            return [];
        }

        $jsonC = $this->ffi->pardox_to_json_arrays($this->ptr);
        if ($jsonC === null) {
            return [];
        }

        $json = \FFI::string($jsonC);
        $this->ffi->pardox_free_string($jsonC);
        return json_decode($json, true) ?? [];
    }

    /**
     * Returns the frequency of each unique value in the given column.
     *
     * @param string $col  Column name.
     * @return array  Associative array ['value' => count, ...] sorted by count desc.
     */
    public function value_counts(string $col): array
    {
        if ($this->ptr === null) {
            return [];
        }

        $jsonC = $this->ffi->pardox_value_counts($this->ptr, $col);
        if ($jsonC === null) {
            return [];
        }

        $json = \FFI::string($jsonC);
        $this->ffi->pardox_free_string($jsonC);
        return json_decode($json, true) ?? [];
    }

    /**
     * Returns the unique values of the given column in insertion order.
     *
     * @param string $col  Column name.
     * @return array  Flat array of unique values.
     */
    public function unique(string $col): array
    {
        if ($this->ptr === null) {
            return [];
        }

        $jsonC = $this->ffi->pardox_unique($this->ptr, $col);
        if ($jsonC === null) {
            return [];
        }

        $json = \FFI::string($jsonC);
        $this->ffi->pardox_free_string($jsonC);
        return json_decode($json, true) ?? [];
    }
}
