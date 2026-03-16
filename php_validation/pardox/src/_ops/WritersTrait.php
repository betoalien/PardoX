<?php

namespace PardoX\DataFrameOps;

trait WritersTrait
{
    /**
     * Export to a CSV file.
     */
    public function to_csv(string $path): bool
    {
        if ($this->ptr === null) {
            return false;
        }

        $res = $this->ffi->pardox_to_csv($this->ptr, $path);

        if ($res !== 1) {
            $errors = [
                -1 => 'Invalid Manager Pointer',
                -2 => 'Invalid Path String',
                -3 => 'Failed to initialize CSV Writer (check permissions)',
                -4 => 'Failed to write header',
                -5 => 'Failed to write data block',
                -6 => 'Failed to flush buffer to disk',
            ];
            $msg = $errors[(int)$res] ?? "Unknown error code: $res";
            throw new \RuntimeException("to_csv failed: $msg");
        }

        return true;
    }

    /**
     * Export to native PardoX binary format (.prdx).
     */
    public function to_prdx(string $path): bool
    {
        if ($this->ptr === null) {
            return false;
        }

        $res = $this->ffi->pardox_to_prdx($this->ptr, $path);

        if ($res !== 1) {
            throw new \RuntimeException("to_prdx failed with code: $res");
        }

        return true;
    }

    /**
     * Write the DataFrame to a PostgreSQL table.
     *
     * @param string $connectionString  PostgreSQL connection URL.
     * @param string $tableName         Target table.
     * @param string $mode              'append' (default) or 'upsert'.
     * @param array  $conflictCols      Columns for ON CONFLICT clause (upsert only).
     * @return int Number of rows written.
     */
    public function to_sql(
        string $connectionString,
        string $tableName,
        string $mode = 'append',
        array  $conflictCols = []
    ): int {
        if (!in_array($mode, ['append', 'upsert'], true)) {
            throw new \InvalidArgumentException("mode must be 'append' or 'upsert'.");
        }

        if ($this->ptr === null) {
            return 0;
        }

        $colsJson = json_encode($conflictCols);

        $rows = $this->ffi->pardox_write_sql(
            $this->ptr,
            $connectionString,
            $tableName,
            $mode,
            $colsJson
        );

        if ($rows < 0) {
            $errors = [
                -1   => 'Invalid Manager Pointer',
                -2   => 'Invalid connection string',
                -3   => 'Invalid table name',
                -4   => 'Invalid mode string',
                -5   => 'Invalid conflict_cols JSON',
                -100 => 'Write operation failed (check connection and table schema)',
            ];
            $msg = $errors[(int)$rows] ?? "Unknown error code: $rows";
            throw new \RuntimeException("to_sql failed: $msg");
        }

        return (int) $rows;
    }

    public function to_mysql(
        string $connectionString,
        string $tableName,
        string $mode = 'append',
        array  $conflictCols = []
    ): int {
        if ($this->ptr === null) { return 0; }
        $colsJson = json_encode($conflictCols);
        $rows = $this->ffi->pardox_write_mysql($this->ptr, $connectionString, $tableName, $mode, $colsJson);
        if ($rows < 0) {
            throw new \RuntimeException("to_mysql failed with error code: $rows");
        }
        return (int) $rows;
    }

    public function to_sqlserver(
        string $connectionString,
        string $tableName,
        string $mode = 'append',
        array  $conflictCols = []
    ): int {
        if ($this->ptr === null) { return 0; }
        $colsJson = json_encode($conflictCols);
        $rows = $this->ffi->pardox_write_sqlserver($this->ptr, $connectionString, $tableName, $mode, $colsJson);
        if ($rows < 0) {
            throw new \RuntimeException("to_sqlserver failed with error code: $rows");
        }
        return (int) $rows;
    }

    public function to_mongodb(
        string $connectionString,
        string $dbDotCollection,
        string $mode = 'append'
    ): int {
        if ($this->ptr === null) { return 0; }
        $rows = $this->ffi->pardox_write_mongodb($this->ptr, $connectionString, $dbDotCollection, $mode);
        if ($rows < 0) {
            throw new \RuntimeException("to_mongodb failed with error code: $rows");
        }
        return (int) $rows;
    }

    /**
     * Write the DataFrame to a Parquet file.
     *
     * @param string $path Destination file path.
     * @return int Number of rows written.
     */
    public function toParquet(string $path): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_to_parquet($this->ptr, $path);
        if ($result < 0) {
            throw new \RuntimeException("toParquet failed with code: $result");
        }
        return (int) $result;
    }

    /**
     * Write the DataFrame to multiple sharded Parquet files.
     *
     * @param string $directory       Output directory path.
     * @param int    $maxRowsPerShard Maximum rows per shard file.
     * @return int Number of rows written.
     */
    public function writeShardedParquet(string $directory, int $maxRowsPerShard = 1000000): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_write_sharded_parquet($this->ptr, $directory, $maxRowsPerShard);
        if ($result < 0) {
            throw new \RuntimeException("writeShardedParquet failed with code: $result");
        }
        return (int) $result;
    }
}
