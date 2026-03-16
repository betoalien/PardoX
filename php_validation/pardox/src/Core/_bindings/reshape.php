<?php
// Core/_bindings/reshape.php — Gap 8 & 10: Pivot, Melt, Explode, Unnest, JSON extract
return <<<'CDECL'

            // --- Gap 8: Pivot & Melt ---
            HyperBlockManager* pardox_pivot_table(HyperBlockManager* mgr, const char* index_col, const char* columns_col, const char* values_col, const char* agg_func);
            HyperBlockManager* pardox_melt(HyperBlockManager* mgr, const char* id_vars_json, const char* value_vars_json, const char* var_name, const char* value_name);

            // --- Gap 10: Nested Data ---
            HyperBlockManager* pardox_json_extract(HyperBlockManager* mgr, const char* col, const char* key_path);
            HyperBlockManager* pardox_explode(HyperBlockManager* mgr, const char* col);
            HyperBlockManager* pardox_unnest(HyperBlockManager* mgr, const char* col);

CDECL;
