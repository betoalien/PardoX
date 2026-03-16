<?php
// Core/_bindings/gpu.php — Gap 7: GPU Compute
return <<<'CDECL'

            // --- Gap 7: GPU Compute ---
            HyperBlockManager* pardox_gpu_add(HyperBlockManager* mgr, const char* col_a, const char* col_b);
            HyperBlockManager* pardox_gpu_sub(HyperBlockManager* mgr, const char* col_a, const char* col_b);
            HyperBlockManager* pardox_gpu_mul(HyperBlockManager* mgr, const char* col_a, const char* col_b);
            HyperBlockManager* pardox_gpu_div(HyperBlockManager* mgr, const char* col_a, const char* col_b);
            HyperBlockManager* pardox_gpu_sqrt(HyperBlockManager* mgr, const char* col);
            HyperBlockManager* pardox_gpu_exp(HyperBlockManager* mgr, const char* col);
            HyperBlockManager* pardox_gpu_log(HyperBlockManager* mgr, const char* col);
            HyperBlockManager* pardox_gpu_abs(HyperBlockManager* mgr, const char* col);

CDECL;
