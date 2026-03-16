<?php
// Core/_bindings/live_query.php — Gap 16: Live Query
return <<<'CDECL'

            // --- Gap 16: Live Query ---
            void* pardox_live_query_start(const char* prdx_path, const char* sql_query, int poll_ms);
            int pardox_live_query_version(void* lq);
            HyperBlockManager* pardox_live_query_take(void* lq);
            void pardox_live_query_free(void* lq);

CDECL;
