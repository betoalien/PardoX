<?php

namespace PardoX\Core;

use FFI as PhpFFI;
use RuntimeException;

class FFI
{
    private static ?PhpFFI $instance = null;

    /**
     * Singleton: Loads the PardoX Engine via FFI.
     *
     * @return PhpFFI The raw FFI instance.
     * @throws RuntimeException If FFI extension is missing or loading fails.
     */
    public static function getInstance(): PhpFFI
    {
        if (self::$instance !== null) {
            return self::$instance;
        }

        if (!extension_loaded('ffi')) {
            throw new RuntimeException("PardoX requires the 'ffi' extension in php.ini (ffi.enable=true).");
        }

        $libPath = Lib::getLibraryPath();

        // Assemble C header from base typedefs + all binding files
        $bindingsDir = __DIR__ . '/_bindings/';

        $cHeader  = "typedef void HyperBlockManager;\n";
        $cHeader .= "typedef unsigned char uint8_t;\n";
        $cHeader .= "typedef unsigned short uint16_t;\n";
        $cHeader .= "typedef unsigned int uint32_t;\n";
        $cHeader .= "typedef unsigned long long uint64_t;\n";
        $cHeader .= "typedef long long int64_t;\n";
        $cHeader .= "typedef int int32_t;\n";
        $cHeader .= "typedef unsigned long size_t;\n";

        foreach ([
            'core', 'inspection', 'compute', 'mutation', 'observer',
            'math_ops', 'strings', 'datetime', 'decimal', 'window',
            'lazy', 'gpu', 'groupby', 'prdx_io', 'persistence',
            'sql_query', 'databases', 'encryption', 'contracts', 'timetravel',
            'reshape', 'timeseries', 'spill', 'cloud', 'flight',
            'cluster', 'live_query', 'rest', 'linalg', 'misc',
        ] as $module) {
            $cHeader .= require $bindingsDir . $module . '.php';
        }

        try {
            self::$instance = PhpFFI::cdef($cHeader, $libPath);
        } catch (\Exception $e) {
            throw new RuntimeException("Fatal Error loading PardoX: " . $e->getMessage());
        }

        // Warmup handshake — mirrors pardox_init_engine() call in wrapper.py
        try {
            self::$instance->pardox_init_engine();
        } catch (\Throwable $e) {
            // Non-fatal: older builds may not export this symbol
        }

        return self::$instance;
    }
}
