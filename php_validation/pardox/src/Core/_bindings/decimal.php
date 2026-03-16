<?php
// Core/_bindings/decimal.php — Gap 3: Decimal type
return <<<'CDECL'

            // --- Gap 3: Decimal Type ---
            HyperBlockManager* pardox_decimal_from_float(HyperBlockManager* mgr, const char* col_name, uint8_t scale);
            HyperBlockManager* pardox_decimal_to_float(HyperBlockManager* mgr, const char* col_name);
            HyperBlockManager* pardox_decimal_add(HyperBlockManager* mgr, const char* col_a, const char* col_b);
            HyperBlockManager* pardox_decimal_sub(HyperBlockManager* mgr, const char* col_a, const char* col_b);
            HyperBlockManager* pardox_decimal_mul_float(HyperBlockManager* mgr, const char* col_name, double factor);
            HyperBlockManager* pardox_decimal_round(HyperBlockManager* mgr, const char* col_name, uint8_t decimals);
            double pardox_decimal_sum(HyperBlockManager* mgr, const char* col_name);
            int pardox_decimal_get_scale(HyperBlockManager* mgr, const char* col_name);

CDECL;
