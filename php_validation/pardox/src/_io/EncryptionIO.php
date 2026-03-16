<?php

namespace PardoX\IOOps;

trait EncryptionIOTrait
{
    // =========================================================================
    // GAP 18: ENCRYPTION
    // =========================================================================

    /**
     * Read a password-encrypted .prdx file into a DataFrame.
     *
     * @param string $path     Path to the encrypted .prdx file.
     * @param string $password Decryption password.
     * @return \PardoX\DataFrame
     */
    public static function readPrdxEncrypted(string $path, string $password): \PardoX\DataFrame
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_read_prdx_encrypted($path, $password);
        if ($ptr === null) throw new \RuntimeException("readPrdxEncrypted failed for: $path");
        return new \PardoX\DataFrame($ptr);
    }

    /**
     * Write a DataFrame to a password-encrypted .prdx file.
     *
     * @param \PardoX\DataFrame $df       DataFrame to encrypt and write.
     * @param string            $path     Destination file path.
     * @param string            $colsJson JSON array of column names to encrypt (or empty for all).
     * @param string            $password Encryption password.
     * @return int Result code (0 = success, negative = error).
     */
    public static function writePrdxEncrypted(\PardoX\DataFrame $df, string $path, string $colsJson, string $password): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_write_prdx_encrypted($df->getPtr(), $path, $colsJson, $password);
    }
}
