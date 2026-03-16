<?php

namespace PardoX\IOOps;

trait PrdxIOTrait
{
    /**
     * Read a native PardoX (.prdx) binary file and return a preview as a PHP array.
     *
     * Note: In v0.1 this uses the native head reader and returns a list of
     * associative arrays rather than a full DataFrame (mirrors Python's read_prdx).
     *
     * @param string $path   Path to the .prdx file.
     * @param int    $limit  Maximum number of rows to read.
     * @return array  List of associative arrays representing data rows.
     */
    public static function read_prdx(string $path, int $limit = 100): array
    {
        if (!file_exists($path)) {
            throw new \RuntimeException("File not found: $path");
        }

        $ffi = \PardoX\Core\FFI::getInstance();

        $jsonC = $ffi->pardox_read_head_json($path, $limit);

        if ($jsonC === null) {
            throw new \RuntimeException("Failed to read PRDX file (Rust returned null): $path");
        }

        // pardox_read_head_json uses a thread-local buffer (not heap-allocated),
        // so pardox_free_string must NOT be called on this pointer.
        $json = \FFI::string($jsonC);

        $data = json_decode($json, true);

        if ($data === null) {
            throw new \RuntimeException("Failed to decode PRDX data as JSON.");
        }

        return $data;
    }

    /**
     * Load a .prdx file into a DataFrame (Gap 12 - full load into memory).
     *
     * @param string $path       Path to the .prdx file.
     * @param int    $limitRows  Maximum rows to read (0 or negative = read all).
     * @return \PardoX\DataFrame|null
     */
    public static function load_prdx(string $path, int $limitRows = 0): ?\PardoX\DataFrame
    {
        if (!file_exists($path)) {
            return null;
        }

        $ffi = \PardoX\Core\FFI::getInstance();

        $mgr = $ffi->pardox_load_manager_prdx($path, $limitRows);

        if ($mgr === null) {
            return null;
        }

        return new \PardoX\DataFrame($mgr);
    }

    /**
     * Read the schema of a .prdx file without loading data.
     *
     * @param string $path Path to the .prdx file.
     * @return array Schema as associative array.
     */
    public static function readPrdxSchema(string $path): array
    {
        if (!file_exists($path)) {
            throw new \RuntimeException("File not found: $path");
        }
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_read_schema($path);
        return $result ? json_decode($result, true) : [];
    }

    /**
     * Run a SQL query directly on a .prdx file.
     *
     * @param string $path Path to the .prdx file.
     * @param string $sql  SQL query string.
     * @return array List of row arrays.
     */
    public static function querySqlPrdx(string $path, string $sql): array
    {
        if (!file_exists($path)) {
            throw new \RuntimeException("File not found: $path");
        }
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_query_sql($path, $sql);
        return $result ? json_decode($result, true) : [];
    }

    /**
     * Compute the sum of a column directly from a .prdx file.
     *
     * @param string $path Path to the .prdx file.
     * @param string $col  Column name.
     * @return float Column sum.
     */
    public static function prdxColumnSum(string $path, string $col): float
    {
        if (!file_exists($path)) {
            throw new \RuntimeException("File not found: $path");
        }
        $ffi = \PardoX\Core\FFI::getInstance();
        return (float) $ffi->pardox_column_sum($path, $col);
    }

    /**
     * Get the minimum value of a numeric column in a .prdx file (streaming, no full load).
     *
     * @param string $path  Path to the .prdx file.
     * @param string $col   Column name.
     * @return float  The minimum value.
     */
    public static function prdx_min(string $path, string $col): float
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_prdx_min($path, $col);
        // pardox_prdx_min returns -1.0 on error; we preserve that signal
        return (float) $result;
    }

    /** @return float Min value of column from .prdx file */
    public static function prdxMin(string $path, string $col): float
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (float) $ffi->pardox_prdx_min($path, $col);
    }

    /**
     * Get the maximum value of a numeric column in a .prdx file (streaming, no full load).
     *
     * @param string $path  Path to the .prdx file.
     * @param string $col   Column name.
     * @return float  The maximum value.
     */
    public static function prdx_max(string $path, string $col): float
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_prdx_max($path, $col);
        return (float) $result;
    }

    /** @return float Max value of column from .prdx file */
    public static function prdxMax(string $path, string $col): float
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (float) $ffi->pardox_prdx_max($path, $col);
    }

    /**
     * Get the mean (average) value of a numeric column in a .prdx file (streaming, no full load).
     *
     * @param string $path  Path to the .prdx file.
     * @param string $col   Column name.
     * @return float  The mean value.
     */
    public static function prdx_mean(string $path, string $col): float
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_prdx_mean($path, $col);
        return (float) $result;
    }

    /** @return float Mean value of column from .prdx file */
    public static function prdxMean(string $path, string $col): float
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (float) $ffi->pardox_prdx_mean($path, $col);
    }

    /**
     * Get the total row count from a .prdx file footer (zero I/O scan).
     *
     * @param string $path  Path to the .prdx file.
     * @return int  Total row count, or -1 on error.
     */
    public static function prdx_count(string $path): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_prdx_count($path);
        return (int) $result;
    }

    /** @return int Row count of .prdx file */
    public static function prdxCount(string $path): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_prdx_count($path);
    }

    /**
     * Perform a streaming GroupBy aggregation over a .prdx file.
     *
     * Reads one row group at a time — memory is O(distinct groups), not O(rows).
     * This allows processing files larger than available RAM.
     *
     * @param string $path         Path to the .prdx file.
     * @param array  $groupCols    Column names to group by (e.g. ['entity', 'category']).
     * @param array  $aggDict      Aggregation spec: ['value_col' => 'func', ...].
     *                             Functions: 'sum', 'count', 'mean', 'min', 'max', 'std'.
     * @return \PardoX\DataFrame|null  DataFrame with group keys and aggregation results, or null on error.
     */
    public static function prdx_groupby(string $path, array $groupCols, array $aggDict): ?\PardoX\DataFrame
    {
        $ffi = \PardoX\Core\FFI::getInstance();

        $groupJson = json_encode($groupCols);
        $aggJson   = json_encode($aggDict);

        $mgr = $ffi->pardox_groupby_agg_prdx($path, $groupJson, $aggJson);

        if ($mgr === null) {
            return null;
        }

        return new \PardoX\DataFrame($mgr);
    }

    /**
     * GroupBy aggregation directly on a .prdx file.
     *
     * @param string $path      Path to .prdx file.
     * @param array  $groupCols Column names to group by.
     * @param array  $agg       Aggregation spec, e.g. ["amount" => "sum"].
     * @return \PardoX\DataFrame
     */
    public static function prdxGroupby(string $path, array $groupCols, array $agg): \PardoX\DataFrame
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_groupby_agg_prdx($path, json_encode($groupCols), json_encode($agg));
        if ($ptr === null) {
            throw new \RuntimeException("prdxGroupby failed for: $path");
        }
        return new \PardoX\DataFrame($ptr);
    }

    /**
     * Lazily inspect a .prdx file and return metadata.
     *
     * @param string $path    Path to the .prdx file.
     * @param array  $options Optional inspection options.
     * @return array File metadata.
     */
    public static function inspectPrdx(string $path, array $options = []): array
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_inspect_file_lazy($path, json_encode($options));
        return $result ? json_decode($result, true) : [];
    }
}
