<?php

namespace PardoX\IOOps;

trait ParquetIOTrait
{
    /**
     * Load a Parquet file into a PardoX DataFrame.
     *
     * @param string $path Path to the .parquet file.
     * @return \PardoX\DataFrame
     */
    public static function readParquet(string $path): \PardoX\DataFrame
    {
        if (!file_exists($path)) {
            throw new \RuntimeException("File not found: $path");
        }
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_load_manager_parquet($path);
        if ($ptr === null) {
            throw new \RuntimeException("readParquet failed for: $path");
        }
        return new \PardoX\DataFrame($ptr);
    }
}
