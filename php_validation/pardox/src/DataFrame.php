<?php

namespace PardoX;

use PardoX\Core\FFI;
use RuntimeException;
use FFI\CData;

// Load all DataFrame operation traits
require_once __DIR__ . '/_ops/VisualizationTrait.php';
require_once __DIR__ . '/_ops/MetadataTrait.php';
require_once __DIR__ . '/_ops/SelectionTrait.php';
require_once __DIR__ . '/_ops/MutationTrait.php';
require_once __DIR__ . '/_ops/WritersTrait.php';
require_once __DIR__ . '/_ops/ExportTrait.php';
require_once __DIR__ . '/_ops/MathOpsTrait.php';
require_once __DIR__ . '/_ops/GpuTrait.php';
require_once __DIR__ . '/_ops/ReshapeTrait.php';
require_once __DIR__ . '/_ops/TimeseriesTrait.php';
require_once __DIR__ . '/_ops/NestedTrait.php';
require_once __DIR__ . '/_ops/SpillTrait.php';
require_once __DIR__ . '/_ops/GroupByTrait.php';
require_once __DIR__ . '/_ops/StringsTrait.php';
require_once __DIR__ . '/_ops/DateTimeTrait.php';
require_once __DIR__ . '/_ops/DecimalTrait.php';
require_once __DIR__ . '/_ops/WindowTrait.php';
require_once __DIR__ . '/_ops/SqlTrait.php';
require_once __DIR__ . '/_ops/EncryptionTrait.php';
require_once __DIR__ . '/_ops/ContractsTrait.php';
require_once __DIR__ . '/_ops/TimeTravelTrait.php';
require_once __DIR__ . '/_ops/ClusterTrait.php';
require_once __DIR__ . '/_ops/LinalgTrait.php';

/**
 * PardoX DataFrame — columnar in-memory table backed by the Rust engine.
 *
 * Implements ArrayAccess so columns can be selected and assigned with
 * the familiar $df['col'] syntax.
 */
class DataFrame implements \ArrayAccess
{
    use \PardoX\DataFrameOps\VisualizationTrait;
    use \PardoX\DataFrameOps\MetadataTrait;
    use \PardoX\DataFrameOps\SelectionTrait;
    use \PardoX\DataFrameOps\MutationTrait;
    use \PardoX\DataFrameOps\WritersTrait;
    use \PardoX\DataFrameOps\ExportTrait;
    use \PardoX\DataFrameOps\MathOpsTrait;
    use \PardoX\DataFrameOps\GpuTrait;
    use \PardoX\DataFrameOps\ReshapeTrait;
    use \PardoX\DataFrameOps\TimeseriesTrait;
    use \PardoX\DataFrameOps\NestedTrait;
    use \PardoX\DataFrameOps\SpillTrait;
    use \PardoX\DataFrameOps\GroupByTrait;
    use \PardoX\DataFrameOps\StringsTrait;
    use \PardoX\DataFrameOps\DateTimeTrait;
    use \PardoX\DataFrameOps\DecimalTrait;
    use \PardoX\DataFrameOps\WindowTrait;
    use \PardoX\DataFrameOps\SqlTrait;
    use \PardoX\DataFrameOps\EncryptionTrait;
    use \PardoX\DataFrameOps\ContractsTrait;
    use \PardoX\DataFrameOps\TimeTravelTrait;
    use \PardoX\DataFrameOps\ClusterTrait;
    use \PardoX\DataFrameOps\LinalgTrait;

    /** @var \FFI */
    private $ffi;

    /** @var CData|null  Opaque pointer to the Rust HyperBlockManager */
    private ?CData $ptr = null;

    // -------------------------------------------------------------------------
    // Construction & Destruction
    // -------------------------------------------------------------------------

    /**
     * @param array|CData|null $data  Array of assoc arrays, an existing CData
     *                                pointer (internal use), or null for empty.
     */
    public function __construct($data = null)
    {
        $this->ffi = FFI::getInstance();

        if ($data === null) {
            return;
        }

        if (is_array($data)) {
            $this->ingestArray($data);
            return;
        }

        // Internal: wrap an existing Rust pointer
        if ($data instanceof CData) {
            $this->ptr = $data;
            return;
        }

        throw new \InvalidArgumentException("DataFrame expects an array, CData pointer, or null.");
    }

    public function __destruct()
    {
        if ($this->ptr !== null) {
            $this->ffi->pardox_free_manager($this->ptr);
            $this->ptr = null;
        }
    }

    // -------------------------------------------------------------------------
    // Magic properties: shape, columns, dtypes
    // -------------------------------------------------------------------------

    public function __get(string $name)
    {
        switch ($name) {
            case 'shape':   return $this->getShape();
            case 'columns': return $this->getColumns();
            case 'dtypes':  return $this->getDtypes();
            default:
                throw new \BadMethodCallException("Undefined property: DataFrame::\$$name");
        }
    }

    // -------------------------------------------------------------------------
    // Factory Methods
    // -------------------------------------------------------------------------

    /**
     * Load a CSV file into a DataFrame using the Rust engine.
     *
     * @param string     $path   Path to the CSV file.
     * @param array|null $schema Associative array ['col' => 'Int64', ...] (optional).
     */
    public static function read_csv(string $path, ?array $schema = null): self
    {
        if (!file_exists($path)) {
            throw new RuntimeException("CSV file not found: $path");
        }

        $ffi = FFI::getInstance();

        $config = json_encode([
            'delimiter'  => 44,
            'quote_char' => 34,
            'has_header' => true,
            'chunk_size' => 16 * 1024 * 1024,
        ]);

        if ($schema !== null) {
            // Convert ['col_name' => 'Int64', ...] to {'column_names': [...], 'column_types': [...]}
            // This is the format expected by the Rust core
            $column_names = [];
            $column_types = [];
            foreach ($schema as $colName => $colType) {
                $column_names[] = $colName;
                $column_types[] = $colType;
            }
            $schemaJson = json_encode(['column_names' => $column_names, 'column_types' => $column_types]);
        } else {
            $schemaJson = '{}';
        }

        $ptr = $ffi->pardox_load_manager_csv($path, $schemaJson, $config);

        if ($ptr === null) {
            throw new RuntimeException("Failed to load CSV: $path");
        }

        $df = new self();
        $df->ptr = $ptr;
        return $df;
    }

    /**
     * Run a SQL query via the Rust native driver and return the result as a DataFrame.
     *
     * @param string $connectionString  PostgreSQL connection URL.
     * @param string $query             SQL query to execute.
     */
    public static function read_sql(string $connectionString, string $query): self
    {
        $ffi = FFI::getInstance();

        $ptr = $ffi->pardox_scan_sql($connectionString, $query);

        if ($ptr === null) {
            throw new RuntimeException("SQL query failed. Check connection string and query.");
        }

        $df = new self();
        $df->ptr = $ptr;
        return $df;
    }

    public static function read_mysql(string $connectionString, string $query): self
    {
        $ffi = FFI::getInstance();
        $ptr = $ffi->pardox_read_mysql($connectionString, $query);
        if ($ptr === null) {
            throw new RuntimeException("MySQL query failed. Check connection string and query.");
        }
        return new self($ptr);
    }

    public static function read_sqlserver(string $connectionString, string $query): self
    {
        $ffi = FFI::getInstance();
        $ptr = $ffi->pardox_read_sqlserver($connectionString, $query);
        if ($ptr === null) {
            throw new RuntimeException("SQL Server query failed. Check connection string and query.");
        }
        return new self($ptr);
    }

    public static function read_mongodb(string $connectionString, string $dbDotCollection): self
    {
        $ffi = FFI::getInstance();
        $ptr = $ffi->pardox_read_mongodb($connectionString, $dbDotCollection);
        if ($ptr === null) {
            throw new RuntimeException("MongoDB read failed. Check connection string and collection.");
        }
        return new self($ptr);
    }

    // -------------------------------------------------------------------------
    // Internal helpers (also used by Series and traits)
    // -------------------------------------------------------------------------

    /**
     * Internal: expose the raw Rust pointer so Series can pass it to FFI calls.
     * @internal
     */
    public function _ptr(): ?CData
    {
        return $this->ptr;
    }

    /**
     * Public alias for _ptr() — returns the raw Rust HyperBlockManager pointer.
     * Required by IO static methods that accept a DataFrame and need the pointer.
     * @internal
     */
    public function getPtr(): ?CData
    {
        return $this->ptr;
    }

    private function ingestArray(array $data): void
    {
        if (empty($data)) {
            throw new \InvalidArgumentException("Cannot create DataFrame from empty array.");
        }

        $ndjson = '';
        foreach ($data as $row) {
            $ndjson .= json_encode($row) . "\n";
        }

        $len = strlen($ndjson);
        $ptr = $this->ffi->pardox_read_json_bytes($ndjson, $len);

        if ($ptr === null) {
            throw new RuntimeException("Rust engine failed to ingest JSON data.");
        }

        $this->ptr = $ptr;
    }

    private function slice(int $start, int $length): self
    {
        $newPtr = $this->ffi->pardox_slice_manager($this->ptr, $start, $length);

        if ($newPtr === null) {
            throw new RuntimeException("Slice operation returned null.");
        }

        $df = new self();
        $df->ptr = $newPtr;
        return $df;
    }

    private function fetchSchema(): array
    {
        if ($this->ptr === null) {
            return [];
        }

        $jsonC = $this->ffi->pardox_get_schema_json($this->ptr);
        if ($jsonC === null) {
            return [];
        }

        // pardox_get_schema_json uses a thread-local buffer (not heap-allocated),
        // so pardox_free_string must NOT be called on this pointer.
        $json = \FFI::string($jsonC);

        return json_decode($json, true) ?? [];
    }
}
