<?php
// Core/_bindings/misc.php — Engine utilities, bulk copy, mainframe, buffer ingestion
return <<<'CDECL'

            // --- Engine Utilities ---
            void pardox_reset();
            long long pardox_legacy_ping(const char* system_name);
            char* pardox_get_system_report();
            char* pardox_version();

            // --- Bulk CSV to PRDX ---
            long long pardox_hyper_copy_v3(const char* src_pattern, const char* out_path, const char* schema_json, const char* config_json);

            // --- Mainframe Import ---
            HyperBlockManager* pardox_import_mainframe_dat(const char* path, const char* schema_json);

            // --- Raw Buffer Ingestion ---
            HyperBlockManager* pardox_ingest_buffer(const void* buffer_ptr, size_t buffer_len, const char* layout_json, const char* schema_json);

            // --- Lazy file inspection ---
            char* pardox_inspect_file_lazy(const char* path, const char* options_json);

CDECL;
