<?php

namespace PardoX\IOOps;

trait LiveQueryIOTrait
{
    // =========================================================================
    // GAP 16: LIVE QUERY
    // =========================================================================

    /**
     * Start a live query that polls a .prdx file for changes.
     *
     * @param string $prdxPath Path to the .prdx file to watch.
     * @param string $sql      SQL query to execute on each poll.
     * @param int    $pollMs   Polling interval in milliseconds (default 1000).
     * @return mixed Opaque live query handle.
     */
    public static function liveQueryStart(string $prdxPath, string $sql, int $pollMs = 1000)
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return $ffi->pardox_live_query_start($prdxPath, $sql, $pollMs);
    }

    /**
     * Return the current version counter of a live query handle.
     *
     * @param mixed $handle Live query handle from liveQueryStart().
     * @return int Version integer (increments each time data changes).
     */
    public static function liveQueryVersion($handle): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_live_query_version($handle);
    }

    /**
     * Take the latest result from a live query as a DataFrame.
     *
     * @param mixed $handle Live query handle from liveQueryStart().
     * @return \PardoX\DataFrame
     */
    public static function liveQueryTake($handle): \PardoX\DataFrame
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_live_query_take($handle);
        if ($ptr === null) throw new \RuntimeException("liveQueryTake returned null.");
        return new \PardoX\DataFrame($ptr);
    }

    /**
     * Free the live query handle and stop polling.
     *
     * @param mixed $handle Live query handle from liveQueryStart().
     */
    public static function liveQueryFree($handle): void
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ffi->pardox_live_query_free($handle);
    }
}
