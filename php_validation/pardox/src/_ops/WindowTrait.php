<?php

namespace PardoX\DataFrameOps;

trait WindowTrait
{
    public function rowNumber(): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_row_number($this->ptr);
        if ($ptr === null) throw new \RuntimeException("rowNumber returned null.");
        return new self($ptr);
    }

    public function rank(string $col): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_rank($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("rank returned null.");
        return new self($ptr);
    }

    public function lag(string $col, int $offset): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_lag($this->ptr, $col, $offset);
        if ($ptr === null) throw new \RuntimeException("lag returned null.");
        return new self($ptr);
    }

    public function lead(string $col, int $offset): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_lead($this->ptr, $col, $offset);
        if ($ptr === null) throw new \RuntimeException("lead returned null.");
        return new self($ptr);
    }

    public function rollingMean(string $col, int $window): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_rolling_mean($this->ptr, $col, $window);
        if ($ptr === null) throw new \RuntimeException("rollingMean returned null.");
        return new self($ptr);
    }
}
