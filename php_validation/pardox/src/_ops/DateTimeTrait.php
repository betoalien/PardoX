<?php

namespace PardoX\DataFrameOps;

trait DateTimeTrait
{
    public function dateExtract(string $col, string $component): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_date_extract($this->ptr, $col, $component);
        if ($ptr === null) throw new \RuntimeException("dateExtract returned null.");
        return new self($ptr);
    }

    public function dateFormat(string $col, string $format): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_date_format($this->ptr, $col, $format);
        if ($ptr === null) throw new \RuntimeException("dateFormat returned null.");
        return new self($ptr);
    }

    public function dateAdd(string $col, int $days): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_date_add($this->ptr, $col, $days);
        if ($ptr === null) throw new \RuntimeException("dateAdd returned null.");
        return new self($ptr);
    }

    public function dateDiff(string $colA, string $colB, string $unit = 'days'): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_date_diff($this->ptr, $colA, $colB, $unit);
        if ($ptr === null) throw new \RuntimeException("dateDiff returned null.");
        return new self($ptr);
    }
}
