<?php
// Core/_bindings/cloud.php — Gap 15: Cloud Storage (S3/GCS/Azure)
return <<<'CDECL'

            // --- Gap 15: Cloud Storage (S3/GCS/Azure) ---
            HyperBlockManager* pardox_cloud_read_csv(const char* url, const char* schema, const char* config, const char* creds);
            HyperBlockManager* pardox_cloud_read_prdx(const char* url, long long limit, const char* creds);
            long long pardox_cloud_write_prdx(HyperBlockManager* mgr, const char* url, const char* creds);

CDECL;
