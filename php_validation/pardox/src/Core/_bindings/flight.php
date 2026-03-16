<?php
// Core/_bindings/flight.php — Gap 21: Arrow Flight
return <<<'CDECL'

            // --- Gap 21: Arrow Flight ---
            long long pardox_flight_start(int port);
            long long pardox_flight_register(const char* name, HyperBlockManager* mgr);
            int pardox_flight_stop();
            HyperBlockManager* pardox_flight_read(const char* server, unsigned short port, const char* dataset);

CDECL;
