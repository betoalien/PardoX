<?php
// Core/_bindings/inspection.php — Row count, schema, JSON/ASCII export, slicing, buffers
return <<<'CDECL'

            // --- Slicing & Navigation ---
            HyperBlockManager* pardox_slice_manager(HyperBlockManager* mgr, size_t start, size_t len);
            HyperBlockManager* pardox_tail_manager(HyperBlockManager* mgr, size_t n);

            // --- Inspection ---
            long long pardox_get_row_count(HyperBlockManager* mgr);
            char* pardox_get_schema_json(HyperBlockManager* mgr);
            char* pardox_manager_to_json(HyperBlockManager* mgr, size_t limit);
            char* pardox_manager_to_ascii(HyperBlockManager* mgr, size_t limit);
            char* pardox_manager_to_json_range(HyperBlockManager* mgr, size_t start, size_t limit);

            // --- Vectorized Read (for validation tests) ---
            double* pardox_get_f64_buffer(HyperBlockManager* mgr, const char* col_name, size_t* out_len);

CDECL;
