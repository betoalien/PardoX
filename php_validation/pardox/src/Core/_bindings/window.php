<?php
// Core/_bindings/window.php — Gap 4: Window functions
return <<<'CDECL'

            // --- Gap 4: Window Functions ---
            HyperBlockManager* pardox_row_number(HyperBlockManager* mgr, const char* order_col, int ascending);
            HyperBlockManager* pardox_rank(HyperBlockManager* mgr, const char* order_col, int ascending);
            HyperBlockManager* pardox_lag(HyperBlockManager* mgr, const char* col_name, long long offset, double fill);
            HyperBlockManager* pardox_lead(HyperBlockManager* mgr, const char* col_name, long long offset, double fill);
            HyperBlockManager* pardox_rolling_mean(HyperBlockManager* mgr, const char* col_name, long long window);

CDECL;
