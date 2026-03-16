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
}
