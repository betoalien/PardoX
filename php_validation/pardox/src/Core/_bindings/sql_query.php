<?php
// Core/_bindings/sql_query.php — Gap 14: SQL query over DataFrame, write SQL, execute SQL
return <<<'CDECL'

            // --- Gap 14: SQL Query over in-memory DataFrame ---
            HyperBlockManager* pardox_sql_query(HyperBlockManager* mgr, const char* sql);

            // --- Writers ---
            long long pardox_write_sql(HyperBlockManager* mgr, const char* conn_str, const char* table, const char* mode, const char* conflict_cols_json);
            long long pardox_execute_sql(const char* conn_str, const char* query);

            // --- Gap 14: Streaming PRDX to SQL ---
            long long pardox_write_sql_prdx(const char* prdx_path, const char* conn_str, const char* table_name, const char* mode, const char* conflict_cols_json, long long batch_rows);

            // --- SQL Server Config Check ---
            int pardox_sqlserver_config_ok();

CDECL;
