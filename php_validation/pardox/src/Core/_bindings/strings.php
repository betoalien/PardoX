<?php
// Core/_bindings/strings.php — Gap 2: String functions
return <<<'CDECL'

            // --- Gap 2: String Functions ---
            HyperBlockManager* pardox_str_upper(HyperBlockManager* mgr, const char* col_name);
            HyperBlockManager* pardox_str_lower(HyperBlockManager* mgr, const char* col_name);
            HyperBlockManager* pardox_str_contains(HyperBlockManager* mgr, const char* col_name, const char* pattern);
            HyperBlockManager* pardox_str_replace(HyperBlockManager* mgr, const char* col_name, const char* from, const char* to);
            HyperBlockManager* pardox_str_trim(HyperBlockManager* mgr, const char* col_name);
            HyperBlockManager* pardox_str_len(HyperBlockManager* mgr, const char* col_name);

CDECL;
