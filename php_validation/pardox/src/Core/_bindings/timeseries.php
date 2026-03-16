<?php
// Core/_bindings/timeseries.php — Gap 9: Time Series Fill
return <<<'CDECL'

            // --- Gap 9: Time Series Fill ---
            HyperBlockManager* pardox_ffill(HyperBlockManager* mgr, const char* col);
            HyperBlockManager* pardox_bfill(HyperBlockManager* mgr, const char* col);
            HyperBlockManager* pardox_interpolate(HyperBlockManager* mgr, const char* col);

CDECL;
