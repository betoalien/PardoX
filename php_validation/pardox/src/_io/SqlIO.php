<?php

namespace PardoX\IOOps;

trait SqlIOTrait
{
    /**
     * Execute a SQL query via the Rust native driver and return the results.
     *
     * Bypasses PHP entirely: Rust connects to the database, executes the query,
     * and fills memory buffers directly before handing back a pointer.
     *
     * @param string $connectionString  PostgreSQL URL (e.g. "postgresql://user:pass@host:5432/db").
     * @param string $query             SQL query (e.g. "SELECT * FROM orders").
     * @return \PardoX\DataFrame
     */
    public static function read_sql(string $connectionString, string $query): \PardoX\DataFrame
    {
        return \PardoX\DataFrame::read_sql($connectionString, $query);
    }

    /**
     * Execute arbitrary DDL or DML directly via the Rust native driver.
     *
     * Bypasses PHP entirely: Rust connects to the database and runs the statement.
     * Useful for CREATE TABLE, DROP TABLE, UPDATE, DELETE, etc.
     *
     * @param string $connectionString  PostgreSQL URL (e.g. "postgresql://user:pass@host:5432/db").
     * @param string $query             SQL statement to execute.
     * @return int  Number of rows affected (0 for DDL statements).
     * @throws \RuntimeException On connection or execution failure.
     */
    public static function executeSql(string $connectionString, string $query): int
    {
        $ffi  = \PardoX\Core\FFI::getInstance();
        $rows = $ffi->pardox_execute_sql($connectionString, $query);

        if ($rows < 0) {
            $errorMap = [
                -1   => 'Invalid connection string',
                -2   => 'Invalid query string',
                -100 => 'Execute operation failed (check connection and SQL syntax)',
            ];
            $msg = $errorMap[(int)$rows] ?? "Unknown error code: $rows";
            throw new \RuntimeException("executeSql failed: $msg");
        }

        return (int) $rows;
    }

    /**
     * Stream a PRDX file directly to PostgreSQL without loading entire file into memory.
     *
     * Gap 14 functionality - streams RowGroups from PRDX to SQL.
     *
     * @param string $prdxPath         Path to the .prdx file.
     * @param string $connectionString PostgreSQL connection URL.
     * @param string $tableName        Target table name.
     * @param string $mode             Write mode - "append" or "replace". Default "append".
     * @param array  $conflictCols     Column names for conflict resolution (upsert).
     * @param int    $batchRows        Number of rows per batch. Default 1,000,000.
     * @return int                     Number of rows written (positive) or error code (negative).
     * @throws \RuntimeException       If PRDX file not found or write fails.
     */
    public static function write_sql_prdx(
        string $prdxPath,
        string $connectionString,
        string $tableName,
        string $mode = 'append',
        array $conflictCols = [],
        int $batchRows = 1000000
    ): int {
        if (!file_exists($prdxPath)) {
            throw new \RuntimeException("PRDX file not found: {$prdxPath}");
        }

        $ffi = \PardoX\Core\FFI::getInstance();
        $conflictJson = json_encode($conflictCols);

        $rows = $ffi->pardox_write_sql_prdx(
            $prdxPath,
            $connectionString,
            $tableName,
            $mode,
            $conflictJson,
            $batchRows
        );

        if ($rows < 0) {
            $errorMap = [
                -1 => 'Invalid PRDX path',
                -2 => 'Invalid connection string',
                -3 => 'Invalid table name',
                -4 => 'Invalid mode',
                -5 => 'Invalid conflict columns JSON',
                -10 => 'Failed to open PRDX file',
                -20 => 'PostgreSQL connection failed',
            ];
            $msg = $errorMap[$rows] ?? "Unknown error code: {$rows}";
            throw new \RuntimeException("write_sql_prdx failed: {$msg}");
        }

        return (int) $rows;
    }

    /**
     * Stream SQL query results as a generator of DataFrames (Gap 30).
     *
     * Reads large SQL tables in batches without loading the entire result into
     * memory. Uses PostgreSQL server-side cursors under the hood.
     *
     * Only `$batchSize` rows are held in RAM per iteration.
     *
     * @param string $connectionString  PostgreSQL URL (e.g. "postgresql://user:pass@host:5432/db").
     * @param string $query             SQL query to stream.
     * @param int    $batchSize         Rows per batch. Default: 100,000.
     * @return \Generator<int, \PardoX\DataFrame>  Yields one DataFrame per batch.
     * @throws \RuntimeException If cursor cannot be opened or batch fetch fails.
     *
     * @example
     *   foreach (IO::queryToResults($conn, 'SELECT * FROM orders', 50000) as $chunk) {
     *       $arrowData = $chunk->toArrow();
     *   }
     */
    public static function queryToResults(
        string $connectionString,
        string $query,
        int $batchSize = 100_000
    ): \Generator {
        $ffi = \PardoX\Core\FFI::getInstance();

        try {
            $cursor = $ffi->pardox_scan_sql_cursor_open($connectionString, $query, $batchSize);
        } catch (\FFI\Exception $e) {
            throw new \RuntimeException(
                'The current PardoX Core build does not export pardox_scan_sql_cursor_open. ' .
                'Re-compile the Rust core with Gap 30 (SQL Cursor API) enabled. ' .
                'FFI error: ' . $e->getMessage()
            );
        }

        if ($cursor === null || (is_int($cursor) && $cursor === 0)) {
            throw new \RuntimeException(
                'pardox_scan_sql_cursor_open returned null. ' .
                'Check the connection string and SQL syntax.'
            );
        }

        try {
            while (true) {
                $mgr = $ffi->pardox_scan_sql_cursor_fetch($cursor);
                if ($mgr === null || (is_int($mgr) && $mgr === 0)) {
                    break;
                }
                yield new \PardoX\DataFrame($mgr);
            }
        } finally {
            $ffi->pardox_scan_sql_cursor_close($cursor);
        }
    }

    /**
     * Export a SQL query directly to multiple Parquet files without loading into RAM (Gap 30).
     *
     * @param string $connectionString  PostgreSQL URL.
     * @param string $query             SQL query to export.
     * @param string $outputPattern     File path pattern with `{i}` placeholder
     *                                  (e.g. `"/data/orders_chunk_{i}.parquet"`).
     * @param int    $chunkSize         Rows per Parquet file. Default: 100,000.
     * @return int                      Total rows exported.
     * @throws \RuntimeException        If the export fails.
     *
     * @example
     *   $total = IO::sqlToParquet($conn, 'SELECT * FROM orders', '/data/orders_{i}.parquet');
     *   echo "Exported {$total} rows\n";
     */
    public static function sqlToParquet(
        string $connectionString,
        string $query,
        string $outputPattern,
        int $chunkSize = 100_000
    ): int {
        $ffi = \PardoX\Core\FFI::getInstance();

        try {
            $total = $ffi->pardox_scan_sql_to_parquet($connectionString, $query, $outputPattern, $chunkSize);
        } catch (\FFI\Exception $e) {
            throw new \RuntimeException(
                'The current PardoX Core build does not export pardox_scan_sql_to_parquet. ' .
                'Re-compile the Rust core with Gap 30 (SQL Cursor API) enabled. ' .
                'FFI error: ' . $e->getMessage()
            );
        }

        if ($total < 0) {
            throw new \RuntimeException("sqlToParquet failed with error code: {$total}");
        }

        return (int) $total;
    }
}
