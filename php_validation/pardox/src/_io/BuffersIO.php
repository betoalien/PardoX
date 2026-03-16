<?php

namespace PardoX\IOOps;

trait BuffersIOTrait
{
    // =========================================================================
    // BULK CSV TO PRDX
    // =========================================================================

    /**
     * Bulk-convert CSV files matching a glob pattern to a .prdx file.
     *
     * @param string $srcPattern Glob pattern for source CSV files.
     * @param string $outPath    Destination .prdx file path.
     * @param array  $schema     Optional schema override.
     * @param array  $config     Optional CSV config.
     * @return int Number of rows copied.
     */
    public static function hyperCopy(string $srcPattern, string $outPath, array $schema = [], array $config = []): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_hyper_copy_v3(
            $srcPattern,
            $outPath,
            json_encode($schema ?: new \stdClass()),
            json_encode($config ?: ['delimiter' => 44, 'quote_char' => 34, 'has_header' => true])
        );
        if ($result < 0) {
            throw new \RuntimeException("hyperCopy failed with code: $result");
        }
        return (int) $result;
    }

    // =========================================================================
    // MAINFRAME IMPORT
    // =========================================================================

    /**
     * Import a mainframe fixed-width .dat file.
     *
     * @param string $path       Path to the .dat file.
     * @param array  $schemaJson Schema descriptor for fixed-width fields.
     * @return \PardoX\DataFrame
     */
    public static function readDat(string $path, array $schemaJson = []): \PardoX\DataFrame
    {
        if (!file_exists($path)) {
            throw new \RuntimeException("File not found: $path");
        }
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_import_mainframe_dat($path, json_encode($schemaJson ?: new \stdClass()));
        if ($ptr === null) {
            throw new \RuntimeException("readDat failed for: $path");
        }
        return new \PardoX\DataFrame($ptr);
    }

    // =========================================================================
    // RAW BUFFER INGESTION
    // =========================================================================

    /**
     * Ingest a raw memory buffer into a DataFrame.
     *
     * @param mixed  $bufferPtr  Pointer to the raw buffer.
     * @param int    $bufferLen  Length of the buffer in bytes.
     * @param array  $layout     Buffer layout descriptor.
     * @param array  $schema     Column schema descriptor.
     * @return \PardoX\DataFrame
     */
    public static function ingestBuffer($bufferPtr, int $bufferLen, array $layout = [], array $schema = []): \PardoX\DataFrame
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_ingest_buffer(
            $bufferPtr,
            $bufferLen,
            json_encode($layout ?: new \stdClass()),
            json_encode($schema ?: new \stdClass())
        );
        if ($ptr === null) {
            throw new \RuntimeException("ingestBuffer returned null.");
        }
        return new \PardoX\DataFrame($ptr);
    }
}
