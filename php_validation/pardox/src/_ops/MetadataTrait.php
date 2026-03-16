<?php

namespace PardoX\DataFrameOps;

trait MetadataTrait
{
    /**
     * Returns [rows, cols]. Alias for getShape().
     */
    public function shape(): array
    {
        return $this->getShape();
    }

    /**
     * Returns [rows, cols].
     */
    public function getShape(): array
    {
        if ($this->ptr === null) {
            return [0, 0];
        }
        $rows = (int) $this->ffi->pardox_get_row_count($this->ptr);
        $cols = count($this->getColumns());
        return [$rows, $cols];
    }

    /**
     * Returns an ordered list of column names.
     */
    private function getColumns(): array
    {
        $schema = $this->fetchSchema();
        return array_column($schema['columns'] ?? [], 'name');
    }

    /**
     * Returns ['col_name' => 'dtype', ...].
     */
    private function getDtypes(): array
    {
        $schema = $this->fetchSchema();
        $result = [];
        foreach ($schema['columns'] ?? [] as $col) {
            $result[$col['name']] = $col['type'];
        }
        return $result;
    }
}
