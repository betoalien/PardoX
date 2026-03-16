<?php

namespace PardoX\DataFrameOps;

trait ContractsTrait
{
    /**
     * Validate the DataFrame against a data contract schema.
     *
     * @param array $rules Associative array of column => rule, e.g. ["amount" => "not_null", "id" => "unique"]
     * @return int Number of violations (0 = all passed)
     */
    public function validateContract(array $rules): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $schemaJson = json_encode($rules);
        return (int) $ffi->pardox_validate_contract($this->ptr, $schemaJson);
    }

    /**
     * Return the number of data contract violations from the last validateContract() call.
     *
     * @return int Violation count.
     */
    public static function contractViolationCount(): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_contract_violation_count();
    }
}
