<?php

namespace PardoX;

use PardoX\Core\FFI;
use RuntimeException;

// Load all IO operation traits
require_once __DIR__ . '/_io/CsvIO.php';
require_once __DIR__ . '/_io/SqlIO.php';
require_once __DIR__ . '/_io/DatabasesIO.php';
require_once __DIR__ . '/_io/PrdxIO.php';
require_once __DIR__ . '/_io/LazyIO.php';
require_once __DIR__ . '/_io/ParquetIO.php';
require_once __DIR__ . '/_io/CloudIO.php';
require_once __DIR__ . '/_io/LiveQueryIO.php';
require_once __DIR__ . '/_io/EncryptionIO.php';
require_once __DIR__ . '/_io/TimeTravelIO.php';
require_once __DIR__ . '/_io/FlightIO.php';
require_once __DIR__ . '/_io/ClusterIO.php';
require_once __DIR__ . '/_io/RestIO.php';
require_once __DIR__ . '/_io/BuffersIO.php';
require_once __DIR__ . '/_io/EngineIO.php';

/**
 * PardoX IO — static factory methods for reading data into DataFrames.
 *
 * Mirrors Python's pardox/io.py module.
 */
class IO
{
    use \PardoX\IOOps\CsvIOTrait;
    use \PardoX\IOOps\SqlIOTrait;
    use \PardoX\IOOps\DatabasesIOTrait;
    use \PardoX\IOOps\PrdxIOTrait;
    use \PardoX\IOOps\LazyIOTrait;
    use \PardoX\IOOps\ParquetIOTrait;
    use \PardoX\IOOps\CloudIOTrait;
    use \PardoX\IOOps\LiveQueryIOTrait;
    use \PardoX\IOOps\EncryptionIOTrait;
    use \PardoX\IOOps\TimeTravelIOTrait;
    use \PardoX\IOOps\FlightIOTrait;
    use \PardoX\IOOps\ClusterIOTrait;
    use \PardoX\IOOps\RestIOTrait;
    use \PardoX\IOOps\BuffersIOTrait;
    use \PardoX\IOOps\EngineIOTrait;

    // Prevent instantiation
    private function __construct() {}
}
