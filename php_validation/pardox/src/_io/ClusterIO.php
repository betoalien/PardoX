<?php

namespace PardoX\IOOps;

trait ClusterIOTrait
{
    // =========================================================================
    // GAP 22: CLUSTER / DISTRIBUTED
    // =========================================================================

    /**
     * Connect to a cluster of PardoX worker nodes.
     *
     * @param array $addresses Worker addresses, e.g. ["127.0.0.1:9001"].
     * @return int Connection handle code.
     */
    public static function clusterConnect(array $addresses): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_cluster_connect(json_encode($addresses));
    }

    /**
     * Ping a cluster worker by name.
     *
     * @param string $workerName Worker node name.
     * @return int Response code.
     */
    public static function clusterPing(string $workerName): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_cluster_ping($workerName);
    }

    /**
     * Ping all connected cluster workers.
     *
     * @return int Number of workers that responded.
     */
    public static function clusterPingAll(): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_cluster_ping_all();
    }

    /**
     * Checkpoint cluster state to disk.
     *
     * @param string $path Destination path for the checkpoint.
     * @return int Result code.
     */
    public static function clusterCheckpoint(string $path): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_cluster_checkpoint($path);
    }

    /**
     * Free the cluster connection.
     */
    public static function clusterFree(): void
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ffi->pardox_cluster_free();
    }

    /**
     * Start a cluster worker node on the given port.
     *
     * @param int $port Port to listen on.
     * @return int Result code.
     */
    public static function clusterWorkerStart(int $port): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_cluster_worker_start($port);
    }

    /**
     * Stop the cluster worker.
     *
     * @return int Result code.
     */
    public static function clusterWorkerStop(): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_cluster_worker_stop();
    }

    /**
     * Register this worker with the cluster.
     *
     * @param string $name       Worker name.
     * @param string $listenAddr Listen address (e.g. "127.0.0.1:9001").
     * @return int Result code.
     */
    public static function clusterWorkerRegister(string $name, string $listenAddr): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_cluster_worker_register($name, $listenAddr);
    }

    /**
     * Scatter a DataFrame across cluster partitions.
     *
     * @param \PardoX\DataFrame $df         DataFrame to scatter.
     * @param int               $partitions Number of partitions.
     * @return array Scatter result metadata.
     */
    public static function clusterScatter(\PardoX\DataFrame $df, int $partitions): array
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_cluster_scatter($df->getPtr(), $partitions);
        return $result ? json_decode($result, true) : [];
    }

    /**
     * Scatter with replication for fault tolerance.
     *
     * @param \PardoX\DataFrame $df                DataFrame to scatter.
     * @param int               $partitions        Number of partitions.
     * @param int               $replicationFactor Replication factor.
     * @return array Scatter result metadata.
     */
    public static function clusterScatterResilient(\PardoX\DataFrame $df, int $partitions, int $replicationFactor = 2): array
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_cluster_scatter_resilient($df->getPtr(), $partitions, $replicationFactor);
        return $result ? json_decode($result, true) : [];
    }

    /**
     * Execute a SQL query across the cluster.
     *
     * @param string $sql SQL query.
     * @return \PardoX\DataFrame
     */
    public static function clusterSql(string $sql): \PardoX\DataFrame
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_cluster_sql($sql);
        if ($ptr === null) {
            throw new \RuntimeException("clusterSql returned null.");
        }
        return new \PardoX\DataFrame($ptr);
    }

    /**
     * Execute a fault-tolerant SQL query across the cluster.
     *
     * @param string $sql SQL query.
     * @return \PardoX\DataFrame
     */
    public static function clusterSqlResilient(string $sql): \PardoX\DataFrame
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_cluster_sql_resilient($sql);
        if ($ptr === null) {
            throw new \RuntimeException("clusterSqlResilient returned null.");
        }
        return new \PardoX\DataFrame($ptr);
    }
}
