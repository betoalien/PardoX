<?php
// Core/_bindings/spill.php — Gap 11: Spill to Disk, external sort, memory usage
return <<<'CDECL'

            // --- Gap 11: Spill to Disk ---
            long long pardox_spill_to_disk(HyperBlockManager* mgr, const char* path);
            HyperBlockManager* pardox_spill_from_disk(const char* path);
            HyperBlockManager* pardox_external_sort(HyperBlockManager* mgr, const char* col, int ascending, long long chunk_size);
            long long pardox_memory_usage(void);

CDECL;
