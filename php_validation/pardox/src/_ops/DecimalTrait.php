<?php

namespace PardoX\DataFrameOps;

trait DecimalTrait
{
    public function decimalFromFloat(string $col, int $scale): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_decimal_from_float($this->ptr, $col, $scale);
        if ($ptr === null) throw new \RuntimeException("decimalFromFloat returned null.");
        return new self($ptr);
    }

    public function decimalToFloat(string $col): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_decimal_to_float($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("decimalToFloat returned null.");
        return new self($ptr);
    }

    public function decimalRound(string $col, int $decimals): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_decimal_round($this->ptr, $col, $decimals);
        if ($ptr === null) throw new \RuntimeException("decimalRound returned null.");
        return new self($ptr);
    }

    public function decimalMulFloat(string $col, float $factor): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_decimal_mul_float($this->ptr, $col, $factor);
        if ($ptr === null) throw new \RuntimeException("decimalMulFloat returned null.");
        return new self($ptr);
    }

    public function decimalSum(string $col): float
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (float) $ffi->pardox_decimal_sum($this->ptr, $col);
    }
}
