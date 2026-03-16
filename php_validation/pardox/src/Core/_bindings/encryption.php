<?php
// Core/_bindings/encryption.php — Gap 18: PRDX encryption
return <<<'CDECL'

            // --- Gap 18: Encryption (PRDX level) ---
            int pardox_write_prdx_encrypted(HyperBlockManager* mgr, const char* path, const char* password);
            HyperBlockManager* pardox_read_prdx_encrypted(const char* path, const char* password);

CDECL;
