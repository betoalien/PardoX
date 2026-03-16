<?php

namespace PardoX\DataFrameOps;

trait SpillTrait
{
    /**
     * Spill the DataFrame to disk in PardoX binary format.
     *
     * @param string $path Path to write the spilled data.
     * @return int Number of bytes written (negative on error).
     */
    public function spill_to_disk(string $path): int
    {
        if ($this->ptr === null) return -1;
        $result = $this->ffi->pardox_spill_to_disk($this->ptr, $path);
        return (int) $result;
    }

    /**
     * Load a DataFrame from a spilled disk file.
     *
     * @param string $path Path to the spilled file.
     * @return self New DataFrame loaded from disk.
     */
    public static function spill_from_disk(string $path): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_spill_from_disk($path);
        if ($ptr === null) throw new \RuntimeException("pardox_spill_from_disk returned null.");
        return new self($ptr);
    }

    /**
     * Perform a chunked GroupBy operation with disk spill for large datasets.
     *
     * @param string $groupCol  Column to group by.
     * @param array  $aggs      Aggregations as ['column' => 'agg_func', ...].
     * @param int    $chunkSize Number of rows to process at a time.
     * @return self New DataFrame with grouped results.
     */
    public function chunked_groupby(string $groupCol, array $aggs, int $chunkSize): self
    {
        $aggJson = json_encode($aggs);
        $ptr = $this->ffi->pardox_chunked_groupby($this->ptr, $groupCol, $aggJson, $chunkSize);
        if ($ptr === null) throw new \RuntimeException("pardox_chunked_groupby returned null.");
        return new self($ptr);
    }

    /**
     * Sort a column using external merge sort with disk spill for large datasets.
     *
     * @param string $col       Column to sort by.
     * @param bool   $ascending Sort order (true = ascending).
     * @param int    $chunkSize Number of rows to sort in memory at a time.
     * @return self New DataFrame sorted by the specified column.
     */
    public function external_sort(string $col, bool $ascending = true, int $chunkSize = 100000): self
    {
        $desc = $ascending ? 1 : 0;
        $ptr = $this->ffi->pardox_external_sort($this->ptr, $col, $desc, $chunkSize);
        if ($ptr === null) throw new \RuntimeException("pardox_external_sort returned null.");
        return new self($ptr);
    }

    /**
     * Get the current memory usage of the PardoX engine in bytes.
     *
     * @return int Memory usage in bytes.
     */
    public static function memory_usage(): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_memory_usage();
    }
}
