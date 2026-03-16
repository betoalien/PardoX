<?php
// Core/_bindings/math_ops.php — Native math foundation, sort
return <<<'CDECL'

            // --- Native Math Foundation (v0.3.1) ---
            HyperBlockManager* pardox_math_add(HyperBlockManager* mgr, const char* col_a, const char* col_b);
            HyperBlockManager* pardox_math_sub(HyperBlockManager* mgr, const char* col_a, const char* col_b);
            double pardox_math_stddev(HyperBlockManager* mgr, const char* col_name);
            HyperBlockManager* pardox_math_minmax(HyperBlockManager* mgr, const char* col_name);

            // --- Sort (v0.3.1) ---
            HyperBlockManager* pardox_sort_values(HyperBlockManager* mgr, const char* col_name, int descending);
            HyperBlockManager* pardox_gpu_sort(HyperBlockManager* mgr, const char* col_name);

CDECL;
