<?php

namespace PardoX\DataFrameOps;

trait VisualizationTrait
{
    /**
     * Return a new DataFrame containing the first N rows.
     */
    public function head(int $n = 5): self
    {
        if ($this->ptr === null) {
            return $this;
        }
        return $this->slice(0, $n);
    }

    /**
     * Return a new DataFrame containing the last N rows.
     */
    public function tail(int $n = 5): self
    {
        if ($this->ptr === null) {
            return $this;
        }

        $newPtr = $this->ffi->pardox_tail_manager($this->ptr, $n);
        if ($newPtr === null) {
            throw new \RuntimeException("tail() failed (Rust returned null).");
        }

        $df = new self();
        $df->ptr = $newPtr;
        return $df;
    }

    /**
     * Print N rows to stdout via Rust's ASCII formatter.
     */
    public function show(int $n = 10): void
    {
        if ($this->ptr === null) {
            echo "<Empty PardoX DataFrame>\n";
            return;
        }

        $asciiC = $this->ffi->pardox_manager_to_ascii($this->ptr, $n);
        if ($asciiC !== null) {
            echo "\n" . \FFI::string($asciiC) . "\n";
            // Note: pardox_manager_to_ascii uses a thread-local buffer (not heap-allocated),
            // so pardox_free_string must NOT be called on this pointer.
        }
    }
}
