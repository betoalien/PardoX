<?php
// Core/_bindings/linalg.php — Gap 28: Linear Algebra
return <<<'CDECL'

            // --- Gap 28: Linear Algebra ---
            double pardox_cosine_sim(HyperBlockManager* left, const char* left_col, HyperBlockManager* right, const char* right_col);
            HyperBlockManager* pardox_l2_normalize(HyperBlockManager* mgr, const char* col_name);
            HyperBlockManager* pardox_l1_normalize(HyperBlockManager* mgr, const char* col_name);
            HyperBlockManager* pardox_matmul(HyperBlockManager* left, const char* left_col, HyperBlockManager* right, const char* right_col);
            HyperBlockManager* pardox_pca(HyperBlockManager* mgr, const char* col_name, int n_components);

CDECL;
