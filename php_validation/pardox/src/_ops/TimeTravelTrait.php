<?php

namespace PardoX\DataFrameOps;

trait TimeTravelTrait
{
    /**
     * Write a versioned snapshot of this DataFrame to disk.
     *
     * @param string $path      Base directory path for version storage.
     * @param string $label     Version label (e.g. "v1", "2024-01-15").
     * @param int    $timestamp Optional Unix timestamp (0 = use current time).
     * @return int Rows written.
     */
    public function versionWrite(string $path, string $label, int $timestamp = 0): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_version_write($this->ptr, $path, $label, $timestamp);
        if ($result < 0) {
            throw new \RuntimeException("versionWrite failed with code: $result");
        }
        return (int) $result;
    }
}
