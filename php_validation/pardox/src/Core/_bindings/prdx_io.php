<?php
// Core/_bindings/prdx_io.php — Gap 12 & 13: PRDX file operations and streaming aggregations
return <<<'CDECL'

            // --- PRDX Ingestion ---
            HyperBlockManager* pardox_load_manager_prdx(const char* path, long long limit);

            // --- Native Readers ---
            char* pardox_read_head_json(const char* path, size_t limit);
            double pardox_column_sum(const char* path, const char* col);
            char* pardox_query_sql(const char* path, const char* query);

            // --- Gap 13: Streaming GroupBy & Aggregations over .prdx ---
            double pardox_prdx_min(const char* path, const char* col);
            double pardox_prdx_max(const char* path, const char* col);
            double pardox_prdx_mean(const char* path, const char* col);
            long long pardox_prdx_count(const char* path);
            HyperBlockManager* pardox_groupby_agg_prdx(const char* path, const char* group_cols_json, const char* agg_json);

            // --- PRDX File Metadata ---
            char* pardox_read_schema(const char* path);
            char* pardox_read_head(const char* path, long long limit);

CDECL;
