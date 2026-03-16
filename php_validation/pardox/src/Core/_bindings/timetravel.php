<?php
// Core/_bindings/timetravel.php — Gap 20: Time Travel / Versioning
return <<<'CDECL'

            // --- Gap 20: Time Travel ---
            long long pardox_version_write(HyperBlockManager* mgr, const char* path, const char* label, long long timestamp);
            HyperBlockManager* pardox_version_read(const char* path, const char* label);
            char* pardox_version_list(const char* path);
            long long pardox_version_delete(const char* path, const char* label);

CDECL;
