<?php

namespace PardoX\IOOps;

trait CloudIOTrait
{
    // =========================================================================
    // GAP 15: CLOUD STORAGE
    // =========================================================================

    /**
     * Read a CSV file from cloud storage (S3/GCS/Azure) into a DataFrame.
     *
     * @param string $url        Cloud URL to the CSV file (e.g. "s3://bucket/file.csv").
     * @param string $schemaJson JSON schema string (default '{}' for auto-inference).
     * @return \PardoX\DataFrame
     */
    public static function cloudReadCsv(string $url, string $schemaJson = '{}'): \PardoX\DataFrame
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_cloud_read_csv($url, $schemaJson);
        if ($ptr === null) throw new \RuntimeException("cloudReadCsv failed for: $url");
        return new \PardoX\DataFrame($ptr);
    }

    /**
     * Read a .prdx file from cloud storage into a DataFrame.
     *
     * @param string $url Cloud URL to the .prdx file.
     * @return \PardoX\DataFrame
     */
    public static function cloudReadPrdx(string $url): \PardoX\DataFrame
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_cloud_read_prdx($url);
        if ($ptr === null) throw new \RuntimeException("cloudReadPrdx failed for: $url");
        return new \PardoX\DataFrame($ptr);
    }

    /**
     * Write a DataFrame to cloud storage as a .prdx file.
     *
     * @param \PardoX\DataFrame $df  DataFrame to write.
     * @param string            $url Cloud destination URL.
     * @return int Number of bytes written (or error code).
     */
    public static function cloudWritePrdx(\PardoX\DataFrame $df, string $url): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_cloud_write_prdx($df->getPtr(), $url);
    }
}
