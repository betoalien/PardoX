'use strict';

const { getLib } = require('../ffi');
const { DataFrame } = require('../DataFrame');

/**
 * Connect to a cluster of PardoX worker nodes.
 * @param {string[]} addresses  Worker addresses.
 * @returns {number}
 */
function clusterConnect(addresses) {
    const lib = getLib();
    return lib.pardox_cluster_connect(JSON.stringify(addresses));
}

/** Ping a cluster worker. */
function clusterPing(workerName) {
    const lib = getLib();
    return lib.pardox_cluster_ping(workerName);
}

/** Ping all cluster workers. */
function clusterPingAll() {
    const lib = getLib();
    return lib.pardox_cluster_ping_all();
}

/** Checkpoint cluster state to disk. */
function clusterCheckpoint(path) {
    const lib = getLib();
    return lib.pardox_cluster_checkpoint(path);
}

/** Free the cluster connection. */
function clusterFree() {
    const lib = getLib();
    lib.pardox_cluster_free();
}

/** Start a cluster worker node. */
function clusterWorkerStart(port) {
    const lib = getLib();
    return lib.pardox_cluster_worker_start(port);
}

/** Stop the cluster worker. */
function clusterWorkerStop() {
    const lib = getLib();
    return lib.pardox_cluster_worker_stop();
}

/** Register worker with the cluster. */
function clusterWorkerRegister(name, listenAddr) {
    const lib = getLib();
    return lib.pardox_cluster_worker_register(name, listenAddr);
}

/**
 * Scatter a DataFrame across cluster partitions.
 * @param {DataFrame} df          DataFrame to scatter.
 * @param {number}    partitions  Number of partitions.
 * @returns {Object|null}
 */
function clusterScatter(df, partitions) {
    const lib    = getLib();
    const result = lib.pardox_cluster_scatter(df._ptr, partitions);
    return result ? JSON.parse(result) : null;
}

/**
 * Scatter with replication for fault tolerance.
 * @param {DataFrame} df                DataFrame.
 * @param {number}    partitions        Number of partitions.
 * @param {number}    replicationFactor Replication factor.
 * @returns {Object|null}
 */
function clusterScatterResilient(df, partitions, replicationFactor = 2) {
    const lib    = getLib();
    const result = lib.pardox_cluster_scatter_resilient(df._ptr, partitions, replicationFactor);
    return result ? JSON.parse(result) : null;
}

/**
 * Execute SQL query across the cluster.
 * @param {string} sql
 * @returns {DataFrame}
 */
function clusterSql(sql) {
    const lib = getLib();
    const ptr = lib.pardox_cluster_sql(sql);
    if (!ptr) throw new Error('clusterSql returned null.');
    return new DataFrame(ptr);
}

/**
 * Execute fault-tolerant SQL across the cluster.
 * @param {string} sql
 * @returns {DataFrame}
 */
function clusterSqlResilient(sql) {
    const lib = getLib();
    const ptr = lib.pardox_cluster_sql_resilient(sql);
    if (!ptr) throw new Error('clusterSqlResilient returned null.');
    return new DataFrame(ptr);
}

module.exports = {
    clusterConnect,
    clusterPing,
    clusterPingAll,
    clusterCheckpoint,
    clusterFree,
    clusterWorkerStart,
    clusterWorkerStop,
    clusterWorkerRegister,
    clusterScatter,
    clusterScatterResilient,
    clusterSql,
    clusterSqlResilient,
};
