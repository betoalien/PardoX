<?php
// Core/_bindings/cluster.php — Gap 22 & 26: Cluster / Distributed / Fault Tolerance
return <<<'CDECL'

            // --- Gap 22: Cluster (extended) ---
            long long pardox_cluster_connect(const char* worker_addresses_json);
            long long pardox_cluster_ping(const char* worker_name);
            long long pardox_cluster_ping_all();
            long long pardox_cluster_checkpoint(const char* path);
            void pardox_cluster_free();
            int pardox_cluster_worker_start(unsigned short port);
            int pardox_cluster_worker_stop();
            int pardox_cluster_worker_register(const char* name, const char* listen_addr);
            char* pardox_cluster_scatter(HyperBlockManager* mgr, int partitions);
            char* pardox_cluster_scatter_resilient(HyperBlockManager* mgr, int partitions, int replication_factor);
            HyperBlockManager* pardox_cluster_sql(const char* sql_query);
            HyperBlockManager* pardox_cluster_sql_resilient(const char* sql_query);

CDECL;
