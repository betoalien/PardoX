<?php
// Core/_bindings/persistence.php — CSV/PRDX/Parquet writers
return <<<'CDECL'

            // --- Writers ---
            long long pardox_to_csv(HyperBlockManager* mgr, const char* path);
            long long pardox_to_prdx(HyperBlockManager* mgr, const char* path);

            // --- Parquet Support ---
            long long pardox_write_sharded_parquet(HyperBlockManager* mgr, const char* directory, long long max_rows_per_shard);
            HyperBlockManager* pardox_load_manager_parquet(const char* path);
            long long pardox_export_to_parquet(HyperBlockManager* mgr, const char* path);
            long long pardox_write_parquet(HyperBlockManager* mgr, const char* path);

CDECL;
