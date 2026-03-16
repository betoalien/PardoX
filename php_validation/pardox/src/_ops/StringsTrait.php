<?php

namespace PardoX\DataFrameOps;

trait StringsTrait
{
    public function strUpper(string $col): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_str_upper($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("strUpper returned null.");
        return new self($ptr);
    }

    public function strLower(string $col): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_str_lower($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("strLower returned null.");
        return new self($ptr);
    }

    public function strLen(string $col): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_str_len($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("strLen returned null.");
        return new self($ptr);
    }

    public function strTrim(string $col): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_str_trim($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("strTrim returned null.");
        return new self($ptr);
    }

    public function strReplace(string $col, string $from, string $to): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_str_replace($this->ptr, $col, $from, $to);
        if ($ptr === null) throw new \RuntimeException("strReplace returned null.");
        return new self($ptr);
    }

    public function strContains(string $col, string $pattern): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_str_contains($this->ptr, $col, $pattern);
        if ($ptr === null) throw new \RuntimeException("strContains returned null.");
        return new self($ptr);
    }
}
