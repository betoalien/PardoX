<?php

namespace PardoX\IOOps;

trait CsvIOTrait
{
    /**
     * Read a CSV file into a PardoX DataFrame using the native Rust engine.
     *
     * @param string     $path    Absolute or relative path to the CSV file.
     * @param array|null $schema  Optional manual schema: ['col_name' => 'Int64', ...].
     *                            Pass null to trigger automatic type inference.
     * @return \PardoX\DataFrame
     */
    public static function read_csv(string $path, ?array $schema = null): \PardoX\DataFrame
    {
        if (!file_exists($path)) {
            throw new \RuntimeException("File not found: $path");
        }

        return \PardoX\DataFrame::read_csv($path, $schema);
    }
}
