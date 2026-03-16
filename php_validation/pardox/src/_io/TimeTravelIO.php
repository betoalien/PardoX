<?php

namespace PardoX\IOOps;

trait TimeTravelIOTrait
{
    // =========================================================================
    // GAP 20: TIME TRAVEL
    // =========================================================================

    /**
     * Read a versioned snapshot from disk.
     *
     * @param string $path  Base directory path.
     * @param string $label Version label.
     * @return \PardoX\DataFrame
     */
    public static function versionRead(string $path, string $label): \PardoX\DataFrame
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_version_read($path, $label);
        if ($ptr === null) {
            throw new \RuntimeException("versionRead failed for path=$path, label=$label");
        }
        return new \PardoX\DataFrame($ptr);
    }

    /**
     * List all version labels for a given base path.
     *
     * @param string $path Base directory path.
     * @return array List of version label strings.
     */
    public static function versionList(string $path): array
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_version_list($path);
        return $result ? json_decode($result, true) : [];
    }

    /**
     * Delete a specific version snapshot.
     *
     * @param string $path  Base directory path.
     * @param string $label Version label to delete.
     * @return int Result code.
     */
    public static function versionDelete(string $path, string $label): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_version_delete($path, $label);
    }
}
