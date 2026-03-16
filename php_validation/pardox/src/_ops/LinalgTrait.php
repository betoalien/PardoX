<?php

namespace PardoX\DataFrameOps;

trait LinalgTrait
{
    /**
     * L2-normalize (unit vector) a numeric column.
     *
     * @param string $col Column name.
     * @return self New DataFrame with normalized column.
     */
    public function linalgL2Normalize(string $col): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_l2_normalize($this->ptr, $col);
        if ($ptr === null) {
            throw new \RuntimeException("linalgL2Normalize returned null.");
        }
        return new self($ptr);
    }

    /**
     * L1-normalize (sum-to-one) a numeric column.
     *
     * @param string $col Column name.
     * @return self New DataFrame with normalized column.
     */
    public function linalgL1Normalize(string $col): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_l1_normalize($this->ptr, $col);
        if ($ptr === null) {
            throw new \RuntimeException("linalgL1Normalize returned null.");
        }
        return new self($ptr);
    }

    /**
     * Compute cosine similarity between two columns across two DataFrames.
     *
     * @param string $colA    Column in this DataFrame.
     * @param self   $other   The other DataFrame.
     * @param string $colB    Column in the other DataFrame.
     * @return float Cosine similarity score.
     */
    public function linalgCosineSim(string $colA, self $other, string $colB): float
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (float) $ffi->pardox_cosine_sim($this->ptr, $colA, $other->ptr, $colB);
    }

    /**
     * Principal Component Analysis (PCA) — reduce a column to N components.
     *
     * @param string $col         Column name containing vector data.
     * @param int    $nComponents Number of principal components.
     * @return self New DataFrame with PCA result.
     */
    public function linalgPca(string $col, int $nComponents): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_pca($this->ptr, $col, $nComponents);
        if ($ptr === null) {
            throw new \RuntimeException("linalgPca returned null.");
        }
        return new self($ptr);
    }

    /**
     * Matrix multiplication between columns of two DataFrames.
     *
     * @param string $colA  Column in this DataFrame.
     * @param self   $other The other DataFrame.
     * @param string $colB  Column in the other DataFrame.
     * @return self New DataFrame with matmul result.
     */
    public function linalgMatmul(string $colA, self $other, string $colB): self
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ptr = $ffi->pardox_matmul($this->ptr, $colA, $other->ptr, $colB);
        if ($ptr === null) {
            throw new \RuntimeException("linalgMatmul returned null.");
        }
        return new self($ptr);
    }
}
