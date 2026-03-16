<?php
// Core/_bindings/groupby.php — Gap 1 & 11: GroupBy aggregations
return <<<'CDECL'

            // --- Gap 1: GroupBy ---
            HyperBlockManager* pardox_groupby_agg(HyperBlockManager* mgr, const char* group_cols_json, const char* agg_json);

            // --- Gap 11: Chunked GroupBy ---
            HyperBlockManager* pardox_chunked_groupby(HyperBlockManager* mgr, const char* group_col, const char* agg_json, long long chunk_size);

CDECL;
