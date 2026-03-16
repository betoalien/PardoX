<?php
// Core/_bindings/observer.php — JSON export, value_counts, unique
return <<<'CDECL'

            // --- Observer v0.3.1: Native Universal Export ---
            char* pardox_to_json_records(HyperBlockManager* mgr);
            char* pardox_to_json_arrays(HyperBlockManager* mgr);

            // --- Observer v0.3.1: Vectorized Inspection (EDA) ---
            char* pardox_value_counts(HyperBlockManager* mgr, const char* col_name);
            char* pardox_unique(HyperBlockManager* mgr, const char* col_name);

CDECL;
