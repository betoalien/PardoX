<?php

namespace PardoX\DataFrameOps;

trait SqlTrait
{
    /**
     * Execute a SQL SELECT query on this DataFrame.
     *
     * Supported SQL subset:
     *   SELECT * | col [AS alias] | AGG(col) [AS alias]
     *   FROM table_name (ignored — queries the current DataFrame)
     *   WHERE col OP value (AND / OR / NOT / BETWEEN / LIKE / IN)
     *   GROUP BY col [, col ...]
     *   HAVING agg_cond
     *   ORDER BY col [ASC | DESC]
     *   LIMIT n
     *
     * Aggregates: SUM, COUNT, AVG/MEAN, MIN, MAX, STD
     *
     * @param string $sql The SQL query to execute.
     * @return self New DataFrame with query results, or null on error.
     */
    public function sql(string $sql): ?self
    {
        if ($this->ptr === null) {
            return null;
        }
        $ptr = $this->ffi->pardox_sql_query($this->ptr, $sql);
        if ($ptr === null) {
            return null;
        }
        return new self($ptr);
    }
}
