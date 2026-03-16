<?php
// Core/_bindings/core.php — Engine init, memory management, ingestion, joins
return <<<'CDECL'

            // --- Engine Initialization ---
            void pardox_init_engine();

            // --- Memory Management ---
            void pardox_free_manager(HyperBlockManager* mgr);
            void pardox_free_string(char* ptr);

            // --- Ingestion ---
            HyperBlockManager* pardox_read_json_bytes(const char* bytes, size_t len);
            HyperBlockManager* pardox_load_manager_csv(const char* path, const char* schema, const char* config);

            // --- Native SQL Read ---
            HyperBlockManager* pardox_scan_sql(const char* conn_str, const char* query);

            // --- Joins ---
            HyperBlockManager* pardox_hash_join(HyperBlockManager* left, HyperBlockManager* right, const char* left_key, const char* right_key);

            // --- Arrow Ingestion ---
            HyperBlockManager* pardox_ingest_arrow_stream(const void* array_ptr, const void* schema_ptr);

CDECL;
