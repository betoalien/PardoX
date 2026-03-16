<?php
// Core/_bindings/lazy.php — Gap 5 & 27: Lazy Pipeline
return <<<'CDECL'

            // --- Gap 5: Lazy Pipeline ---
            void* pardox_lazy_scan_csv(const char* path, int delimiter, int has_header);
            void* pardox_lazy_select(void* frame, const char* cols_json);
            void* pardox_lazy_filter(void* frame, const char* col, const char* op, double value);
            void* pardox_lazy_limit(void* frame, long long limit);
            HyperBlockManager* pardox_lazy_collect(void* frame);
            char* pardox_lazy_describe(void* frame);
            void pardox_lazy_free(void* frame);

            // --- Gap 27: Query Planner (Lazy Pipeline extended) ---
            void* pardox_lazy_scan_prdx(const char* path);
            void* pardox_lazy_optimize(void* frame);
            char* pardox_lazy_stats(void* frame);

CDECL;
