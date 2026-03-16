<?php
// Core/_bindings/databases.php — MySQL, SQL Server, MongoDB native drivers
return <<<'CDECL'

            // --- MySQL ---
            HyperBlockManager* pardox_read_mysql(const char* conn_str, const char* query);
            long long pardox_write_mysql(HyperBlockManager* mgr, const char* conn_str, const char* table, const char* mode, const char* conflict_cols_json);
            long long pardox_execute_mysql(const char* conn_str, const char* query);

            // --- SQL Server ---
            HyperBlockManager* pardox_read_sqlserver(const char* conn_str, const char* query);
            long long pardox_write_sqlserver(HyperBlockManager* mgr, const char* conn_str, const char* table, const char* mode, const char* conflict_cols_json);
            long long pardox_execute_sqlserver(const char* conn_str, const char* query);

            // --- MongoDB ---
            HyperBlockManager* pardox_read_mongodb(const char* conn_str, const char* db_dot_collection);
            long long pardox_write_mongodb(HyperBlockManager* mgr, const char* conn_str, const char* db_dot_collection, const char* mode);
            long long pardox_execute_mongodb(const char* conn_str, const char* database, const char* command_json);

CDECL;
