<?php
// Core/_bindings/compute.php — Filter predicates, aggregations, series arithmetic
return <<<'CDECL'

            // --- Filter Predicates ---
            HyperBlockManager* pardox_filter_compare(HyperBlockManager* left, const char* left_col, HyperBlockManager* right, const char* right_col, int op_code);
            HyperBlockManager* pardox_filter_compare_scalar(HyperBlockManager* mgr, const char* col, double val_f64, long long val_i64, int is_float, int op_code);
            HyperBlockManager* pardox_apply_filter(HyperBlockManager* data, HyperBlockManager* mask, const char* mask_col);

            // --- Aggregations ---
            double pardox_agg_sum(HyperBlockManager* mgr, const char* col);
            double pardox_agg_mean(HyperBlockManager* mgr, const char* col);
            double pardox_agg_min(HyperBlockManager* mgr, const char* col);
            double pardox_agg_max(HyperBlockManager* mgr, const char* col);
            double pardox_agg_count(HyperBlockManager* mgr, const char* col);
            double pardox_agg_std(HyperBlockManager* mgr, const char* col);

            // --- Arithmetic (Series) ---
            HyperBlockManager* pardox_series_add(HyperBlockManager* left, const char* left_col, HyperBlockManager* right, const char* right_col);
            HyperBlockManager* pardox_series_sub(HyperBlockManager* left, const char* left_col, HyperBlockManager* right, const char* right_col);
            HyperBlockManager* pardox_series_mul(HyperBlockManager* left, const char* left_col, HyperBlockManager* right, const char* right_col);
            HyperBlockManager* pardox_series_div(HyperBlockManager* left, const char* left_col, HyperBlockManager* right, const char* right_col);
            HyperBlockManager* pardox_series_mod(HyperBlockManager* left, const char* left_col, HyperBlockManager* right, const char* right_col);

CDECL;
