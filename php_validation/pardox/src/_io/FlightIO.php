<?php

namespace PardoX\IOOps;

trait FlightIOTrait
{
    // =========================================================================
    // GAP 21: ARROW FLIGHT
    // =========================================================================

    /**
     * Start a local Arrow Flight server.
     *
     * @param int $port Port number (default 8815).
     * @return int Result code.
     */
    public static function flightStart(int $port = 8815): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_flight_start($port);
    }

    /**
     * Register a DataFrame on the local Arrow Flight server.
     *
     * @param string             $name Dataset name.
     * @param \PardoX\DataFrame  $df   DataFrame to register.
     * @return int Result code.
     */
    public static function flightRegister(string $name, \PardoX\DataFrame $df): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_flight_register($name, $df->getPtr());
    }

    /**
     * Read a dataset from an Arrow Flight server.
     *
     * @param string $server  Server host address.
     * @param int    $port    Server port.
     * @param string $dataset Dataset name.
     * @return \PardoX\DataFrame
     */
    public static function flightRead(string $server, int $port, string $dataset): \PardoX\DataFrame
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_flight_read($server, $port, $dataset);
        if ($ptr === null) {
            throw new \RuntimeException("flightRead failed: $server:$port/$dataset");
        }
        return new \PardoX\DataFrame($ptr);
    }

    /**
     * Stop the local Arrow Flight server.
     *
     * @return int Result code.
     */
    public static function flightStop(): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_flight_stop();
    }
}
