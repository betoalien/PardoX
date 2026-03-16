<?php
// Core/_bindings/contracts.php — Gap 19: Data Contracts
return <<<'CDECL'

            // --- Gap 19: Data Contracts ---
            long long pardox_validate_contract(HyperBlockManager* mgr, const char* schema_json);
            long long pardox_contract_violation_count();
            char* pardox_get_quarantine_logs();
            void pardox_clear_quarantine();

CDECL;
