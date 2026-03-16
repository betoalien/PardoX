<?php

namespace PardoX\IOOps;

trait RestIOTrait
{
    // =========================================================================
    // GAP 29: REST CONNECTOR
    // =========================================================================

    /**
     * Fetch JSON data from a REST API endpoint into a DataFrame.
     *
     * @param string $url     REST endpoint URL.
     * @param string $method  HTTP method (default "GET").
     * @param array  $headers Optional HTTP headers.
     * @return \PardoX\DataFrame
     */
    public static function readRest(string $url, string $method = 'GET', array $headers = []): \PardoX\DataFrame
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_read_rest($url, $method, json_encode($headers ?: new \stdClass()));
        if ($ptr === null) {
            throw new \RuntimeException("readRest failed for URL: $url");
        }
        return new \PardoX\DataFrame($ptr);
    }
}
