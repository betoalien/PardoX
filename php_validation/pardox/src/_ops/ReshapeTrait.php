<?php

namespace PardoX\DataFrameOps;

trait ReshapeTrait
{
    /**
     * Create a pivot table from the DataFrame.
     *
     * @param string $indexCol   Column to use as the index (row labels).
     * @param string $columnsCol Column to use for the new columns.
     * @param string $valuesCol  Column containing the values to aggregate.
     * @param string $aggFunc    Aggregation function: "sum", "mean", "min", "max", "count".
     * @return self New DataFrame with pivoted structure.
     */
    public function pivot_table(string $indexCol, string $columnsCol, string $valuesCol, string $aggFunc): self
    {
        $ptr = $this->ffi->pardox_pivot_table($this->ptr, $indexCol, $columnsCol, $valuesCol, $aggFunc);
        if ($ptr === null) throw new \RuntimeException("pardox_pivot_table returned null.");
        return new self($ptr);
    }

    /**
     * Melt the DataFrame from wide to long format.
     *
     * @param array      $idVars    Columns to use as identifier variables.
     * @param array      $valueVars Columns to unpivot (melt).
     * @param string     $varName   Name for the new 'variable' column (default: 'variable').
     * @param string     $valueName Name for the new 'value' column (default: 'value').
     * @return self New DataFrame in long format.
     */
    public function melt(array $idVars, array $valueVars, string $varName = 'variable', string $valueName = 'value'): self
    {
        $idJson = json_encode($idVars);
        $valJson = json_encode($valueVars);
        $ptr = $this->ffi->pardox_melt($this->ptr, $idJson, $valJson, $varName, $valueName);
        if ($ptr === null) throw new \RuntimeException("pardox_melt returned null.");
        return new self($ptr);
    }
}
