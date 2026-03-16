<?php
// Core/_bindings/mutation.php — Cast, add column, fill NA, round
return <<<'CDECL'

            // --- Casting & Column Mutation ---
            int pardox_cast_column(HyperBlockManager* mgr, const char* col_name, const char* target_type);
            long long pardox_add_column(HyperBlockManager* target, HyperBlockManager* source, const char* col_name);

            // --- Cleaning ---
            long long pardox_fill_na(HyperBlockManager* mgr, const char* col, double val);
            long long pardox_round(HyperBlockManager* mgr, const char* col, int decimals);

CDECL;
