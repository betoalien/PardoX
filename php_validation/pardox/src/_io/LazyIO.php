<?php

namespace PardoX\IOOps;

trait LazyIOTrait
{
    /**
     * Create a LazyFrame from a CSV file (deferred execution — no data loaded yet).
     *
     * @param string $path       Path to the CSV file.
     * @param int    $delimiter  Delimiter ASCII code (default 44 = comma).
     * @param bool   $hasHeader  Whether the file has a header row.
     * @return mixed Opaque LazyFrame pointer.
     */
    public static function scanCsv(string $path, int $delimiter = 44, bool $hasHeader = true)
    {
        if (!file_exists($path)) {
            throw new \RuntimeException("File not found: $path");
        }
        $ffi = \PardoX\Core\FFI::getInstance();
        return $ffi->pardox_lazy_scan_csv($path, $delimiter, $hasHeader ? 1 : 0);
    }

    /**
     * Create a LazyFrame from a .prdx file (deferred execution).
     *
     * @param string $path Path to the .prdx file.
     * @return mixed Opaque LazyFrame pointer.
     */
    public static function scanPrdx(string $path)
    {
        if (!file_exists($path)) {
            throw new \RuntimeException("File not found: $path");
        }
        $ffi = \PardoX\Core\FFI::getInstance();
        return $ffi->pardox_lazy_scan_prdx($path);
    }

    /**
     * Collect a LazyFrame into a materialized DataFrame.
     *
     * @param mixed $lazyFrame LazyFrame pointer.
     * @return \PardoX\DataFrame
     */
    public static function lazyCollect($lazyFrame): \PardoX\DataFrame
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_lazy_collect($lazyFrame);
        if ($ptr === null) {
            throw new \RuntimeException("lazyCollect returned null.");
        }
        return new \PardoX\DataFrame($ptr);
    }

    /**
     * Apply a column filter predicate to a LazyFrame.
     *
     * @param mixed  $lazyFrame LazyFrame pointer.
     * @param string $col       Column name to filter on.
     * @param string $op        Comparison operator (e.g. "gt", "lt", "eq", "neq", "gte", "lte").
     * @param float  $value     Scalar value to compare against.
     * @return mixed New LazyFrame pointer with filter applied.
     */
    public static function lazyFilter($lazyFrame, string $col, string $op, float $value)
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return $ffi->pardox_lazy_filter($lazyFrame, $col, $op, $value);
    }

    /**
     * Select specific columns in a LazyFrame.
     *
     * @param mixed $lazyFrame LazyFrame pointer.
     * @param array $cols      List of column names to keep.
     * @return mixed New LazyFrame pointer with column selection applied.
     */
    public static function lazySelect($lazyFrame, array $cols)
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return $ffi->pardox_lazy_select($lazyFrame, json_encode($cols));
    }

    /**
     * Limit the number of rows in a LazyFrame.
     *
     * @param mixed $lazyFrame LazyFrame pointer.
     * @param int   $limit     Maximum number of rows to keep.
     * @return mixed New LazyFrame pointer with limit applied.
     */
    public static function lazyLimit($lazyFrame, int $limit)
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return $ffi->pardox_lazy_limit($lazyFrame, $limit);
    }

    /**
     * Apply query planner optimizations to a LazyFrame.
     *
     * @param mixed $lazyFrame LazyFrame pointer from scanCsv() or scanPrdx().
     * @return mixed Optimized LazyFrame pointer.
     */
    public static function lazyOptimize($lazyFrame)
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return $ffi->pardox_lazy_optimize($lazyFrame);
    }

    /**
     * Return statistics about the lazy plan (estimated rows, steps, etc.).
     *
     * @param mixed $lazyFrame LazyFrame pointer.
     * @return array Plan statistics.
     */
    public static function lazyStats($lazyFrame): array
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_lazy_stats($lazyFrame);
        return $result ? json_decode($result, true) : [];
    }
}
