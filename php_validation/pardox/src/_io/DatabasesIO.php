<?php

namespace PardoX\IOOps;

trait DatabasesIOTrait
{
    /**
     * Read from MySQL into a PardoX DataFrame using the native Rust driver.
     *
     * @param string $connectionString  MySQL URL (e.g. "mysql://user:pass@host:3306/db").
     * @param string $query             SQL query to execute.
     * @return \PardoX\DataFrame
     */
    public static function read_mysql(string $connectionString, string $query): \PardoX\DataFrame
    {
        return \PardoX\DataFrame::read_mysql($connectionString, $query);
    }

    /**
     * Execute arbitrary DDL or DML directly against MySQL via the Rust native driver.
     *
     * @param string $connectionString  MySQL URL.
     * @param string $query             SQL statement to execute.
     * @return int  Number of rows affected.
     */
    public static function executeMysql(string $connectionString, string $query): int
    {
        $ffi  = \PardoX\Core\FFI::getInstance();
        $rows = $ffi->pardox_execute_mysql($connectionString, $query);
        if ($rows < 0) {
            throw new \RuntimeException("executeMysql failed with error code: $rows");
        }
        return (int) $rows;
    }

    /**
     * Read from SQL Server into a PardoX DataFrame using the native Rust driver (tiberius).
     *
     * @param string $connectionString  ADO.NET connection string.
     * @param string $query             SQL query to execute.
     * @return \PardoX\DataFrame
     */
    public static function read_sqlserver(string $connectionString, string $query): \PardoX\DataFrame
    {
        return \PardoX\DataFrame::read_sqlserver($connectionString, $query);
    }

    /**
     * Execute arbitrary DDL or DML directly against SQL Server via the Rust native driver.
     *
     * @param string $connectionString  ADO.NET connection string.
     * @param string $query             SQL statement to execute.
     * @return int  Number of rows affected.
     */
    public static function executeSqlserver(string $connectionString, string $query): int
    {
        $ffi  = \PardoX\Core\FFI::getInstance();
        $rows = $ffi->pardox_execute_sqlserver($connectionString, $query);
        if ($rows < 0) {
            throw new \RuntimeException("executeSqlserver failed with error code: $rows");
        }
        return (int) $rows;
    }

    /**
     * Read a MongoDB collection into a PardoX DataFrame using the native Rust driver.
     *
     * @param string $connectionString   MongoDB URI (e.g. "mongodb://user:pass@host:27017").
     * @param string $dbDotCollection    Target as "database.collection" (e.g. "mydb.orders").
     * @return \PardoX\DataFrame
     */
    public static function read_mongodb(string $connectionString, string $dbDotCollection): \PardoX\DataFrame
    {
        return \PardoX\DataFrame::read_mongodb($connectionString, $dbDotCollection);
    }

    /**
     * Execute a MongoDB command via the Rust native driver.
     *
     * @param string $connectionString  MongoDB URI.
     * @param string $database          Target database name.
     * @param string $commandJson       JSON string of the MongoDB command document.
     *                                  e.g. '{"drop":"my_collection"}'
     * @return int  Value of the 'n' or 'ok' field from the result.
     */
    public static function executeMongodb(
        string $connectionString,
        string $database,
        string $commandJson
    ): int {
        $ffi    = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_execute_mongodb($connectionString, $database, $commandJson);
        if ($result < 0) {
            throw new \RuntimeException("executeMongodb failed with error code: $result");
        }
        return (int) $result;
    }
}
