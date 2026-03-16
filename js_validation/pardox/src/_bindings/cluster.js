'use strict';

module.exports = function bindCluster(_lib) {
    _lib._raw_pardox_cluster_connect          = _lib.raw.func('int64_t pardox_cluster_connect(const char * worker_addresses_json)');
    _lib._raw_pardox_cluster_ping             = _lib.raw.func('int64_t pardox_cluster_ping(const char * worker_name)');
    _lib._raw_pardox_cluster_ping_all         = _lib.raw.func('int64_t pardox_cluster_ping_all()');
    _lib._raw_pardox_cluster_checkpoint       = _lib.raw.func('int64_t pardox_cluster_checkpoint(const char * path)');
    _lib.pardox_cluster_free                  = _lib.raw.func('void pardox_cluster_free()');
    _lib.pardox_cluster_worker_start          = _lib.raw.func('int pardox_cluster_worker_start(uint16_t port)');
    _lib.pardox_cluster_worker_stop           = _lib.raw.func('int pardox_cluster_worker_stop()');
    _lib.pardox_cluster_worker_register       = _lib.raw.func('int pardox_cluster_worker_register(const char * name, const char * listen_addr)');
    _lib._raw_pardox_cluster_scatter          = _lib.raw.func('void * pardox_cluster_scatter(void * mgr, int partitions)');
    _lib._raw_pardox_cluster_scatter_resilient = _lib.raw.func('void * pardox_cluster_scatter_resilient(void * mgr, int partitions, int replication_factor)');
    _lib.pardox_cluster_sql                   = _lib.raw.func('void * pardox_cluster_sql(const char * sql_query)');
    _lib.pardox_cluster_sql_resilient         = _lib.raw.func('void * pardox_cluster_sql_resilient(const char * sql_query)');
};
