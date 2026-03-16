<?php
// Core/_bindings/datetime.php — Gap 2: Date functions
return <<<'CDECL'

            // --- Gap 2: Date Functions ---
            HyperBlockManager* pardox_date_extract(HyperBlockManager* mgr, const char* col_name, const char* part);
            HyperBlockManager* pardox_date_format(HyperBlockManager* mgr, const char* col_name, const char* fmt);
            HyperBlockManager* pardox_date_add(HyperBlockManager* mgr, const char* col_name, long long days);
            HyperBlockManager* pardox_date_diff(HyperBlockManager* mgr, const char* col_a, const char* col_b, const char* unit);

CDECL;
