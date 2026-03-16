<?php

namespace PardoX\DataFrameOps;

trait GpuTrait
{
    /**
     * GPU-accelerated column addition: col_a + col_b → new DataFrame with "gpu_add".
     * Tries GPU first; falls back to CPU if GPU is unavailable.
     */
    public function gpu_add(string $colA, string $colB): self
    {
        $ptr = $this->ffi->pardox_gpu_add($this->ptr, $colA, $colB);
        if ($ptr === null) throw new \RuntimeException("pardox_gpu_add returned null.");
        return new self($ptr);
    }

    /**
     * GPU-accelerated column subtraction: col_a - col_b → new DataFrame with "gpu_sub".
     */
    public function gpu_sub(string $colA, string $colB): self
    {
        $ptr = $this->ffi->pardox_gpu_sub($this->ptr, $colA, $colB);
        if ($ptr === null) throw new \RuntimeException("pardox_gpu_sub returned null.");
        return new self($ptr);
    }

    /**
     * GPU-accelerated column multiplication: col_a × col_b → new DataFrame with "gpu_mul".
     */
    public function gpu_mul(string $colA, string $colB): self
    {
        $ptr = $this->ffi->pardox_gpu_mul($this->ptr, $colA, $colB);
        if ($ptr === null) throw new \RuntimeException("pardox_gpu_mul returned null.");
        return new self($ptr);
    }

    /**
     * GPU-accelerated column division: col_a ÷ col_b → new DataFrame with "gpu_div".
     * Division by zero produces NaN (IEEE 754).
     */
    public function gpu_div(string $colA, string $colB): self
    {
        $ptr = $this->ffi->pardox_gpu_div($this->ptr, $colA, $colB);
        if ($ptr === null) throw new \RuntimeException("pardox_gpu_div returned null.");
        return new self($ptr);
    }

    /**
     * GPU-accelerated square root: √(col) → new DataFrame with "gpu_sqrt".
     */
    public function gpu_sqrt(string $col): self
    {
        $ptr = $this->ffi->pardox_gpu_sqrt($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("pardox_gpu_sqrt returned null.");
        return new self($ptr);
    }

    /**
     * GPU-accelerated exponential: e^col → new DataFrame with "gpu_exp".
     */
    public function gpu_exp(string $col): self
    {
        $ptr = $this->ffi->pardox_gpu_exp($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("pardox_gpu_exp returned null.");
        return new self($ptr);
    }

    /**
     * GPU-accelerated natural logarithm: ln(col) → new DataFrame with "gpu_log".
     */
    public function gpu_log(string $col): self
    {
        $ptr = $this->ffi->pardox_gpu_log($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("pardox_gpu_log returned null.");
        return new self($ptr);
    }

    /**
     * GPU-accelerated absolute value: |col| → new DataFrame with "gpu_abs".
     */
    public function gpu_abs(string $col): self
    {
        $ptr = $this->ffi->pardox_gpu_abs($this->ptr, $col);
        if ($ptr === null) throw new \RuntimeException("pardox_gpu_abs returned null.");
        return new self($ptr);
    }
}
