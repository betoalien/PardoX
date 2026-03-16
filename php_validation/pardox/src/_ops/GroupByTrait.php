<?php

namespace PardoX\DataFrameOps;

trait GroupByTrait
{
    // GroupBy operations live on IO (prdx_groupby) and SpillTrait (chunked_groupby).
    // This trait is reserved for any future in-memory groupby DataFrame method.
    // Currently the primary groupby API is via IO::prdx_groupby() for file-level ops
    // and DataFrame::chunked_groupby() (in SpillTrait) for in-memory chunked ops.
}
