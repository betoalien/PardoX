"""
pardox/lazy.py — LazyFrame API (Gap 5: Lazy Pipeline, Gap 27: Query Planner)

LazyFrame enables deferred execution: operations are queued and optimized
before being executed with .collect(). Execution starts from a file scan
(scan_csv or scan_prdx), not from an in-memory DataFrame.

Usage:
    import pardox as pdx

    # Scan from CSV (lazy — nothing is loaded yet)
    lf = pdx.scan_csv("sales.csv")

    # Build a lazy pipeline
    lf = lf.filter("price", "gt", 100.0) \
           .select(["category", "price", "quantity"]) \
           .limit(1000)

    # Execute: optimize + run the plan
    df = lf.collect()
    print(df)
"""

import ctypes
import json

from .wrapper import lib, c_void_p, c_char_p, c_longlong, c_double


class LazyFrame:
    """
    Deferred computation pipeline over PardoX data.

    Operations on a LazyFrame are not executed immediately — they are queued
    and optimized. Call .collect() to execute and get a DataFrame.
    """

    def __init__(self, ptr):
        if not ptr:
            raise RuntimeError("LazyFrame received a null pointer from Core.")
        self._ptr = ptr
        self._freed = False

    # =========================================================================
    # PIPELINE OPERATIONS
    # =========================================================================

    def filter(self, col: str, op: str, value: float):
        """
        Add a filter step to the pipeline.

        Args:
            col (str): Column name.
            op (str): Comparison operator — "eq", "neq", "gt", "gte", "lt", "lte".
            value (float): Scalar value to compare against.

        Returns:
            LazyFrame: New LazyFrame with filter applied.

        Example:
            lf = lf.filter("price", "gt", 100.0)
        """
        if not hasattr(lib, 'pardox_lazy_filter'):
            raise NotImplementedError("pardox_lazy_filter not found in Core.")
        new_ptr = lib.pardox_lazy_filter(
            self._ptr,
            col.encode('utf-8'),
            op.encode('utf-8'),
            c_double(float(value))
        )
        if not new_ptr:
            raise RuntimeError(f"lazy.filter({col!r}, {op!r}, {value}) returned null.")
        # Core mutates in-place and returns the same pointer — return self to avoid double-free
        self._ptr = new_ptr
        return self

    def select(self, cols: list):
        """
        Add a column selection step.

        Args:
            cols (list): List of column names to keep.

        Returns:
            LazyFrame: self (mutated in-place by Core).

        Example:
            lf = lf.select(["category", "price"])
        """
        if not hasattr(lib, 'pardox_lazy_select'):
            raise NotImplementedError("pardox_lazy_select not found in Core.")
        cols_json = json.dumps(cols).encode('utf-8')
        new_ptr = lib.pardox_lazy_select(self._ptr, cols_json)
        if not new_ptr:
            raise RuntimeError(f"lazy.select({cols}) returned null.")
        self._ptr = new_ptr
        return self

    def limit(self, n: int):
        """
        Limit the result to the first n rows.

        Args:
            n (int): Maximum number of rows.

        Returns:
            LazyFrame: self (mutated in-place by Core).
        """
        if not hasattr(lib, 'pardox_lazy_limit'):
            raise NotImplementedError("pardox_lazy_limit not found in Core.")
        new_ptr = lib.pardox_lazy_limit(self._ptr, c_longlong(n))
        if not new_ptr:
            raise RuntimeError(f"lazy.limit({n}) returned null.")
        self._ptr = new_ptr
        return self

    def optimize(self):
        """
        Apply the query planner optimizer to the current pipeline.
        (Gap 27 — predicate pushdown, projection pruning, etc.)

        Returns:
            LazyFrame: self (optimized in-place by Core).
        """
        if not hasattr(lib, 'pardox_lazy_optimize'):
            raise NotImplementedError("pardox_lazy_optimize not found in Core.")
        new_ptr = lib.pardox_lazy_optimize(self._ptr)
        if not new_ptr:
            raise RuntimeError("lazy.optimize() returned null.")
        self._ptr = new_ptr
        return self

    # =========================================================================
    # EXECUTION
    # =========================================================================

    def collect(self):
        """
        Execute the pipeline and return a DataFrame.

        Returns:
            DataFrame: The materialized result.

        Example:
            df = pdx.scan_csv("sales.csv").filter("price", "gt", 100).collect()
            print(df)
        """
        from .frame import DataFrame
        if not hasattr(lib, 'pardox_lazy_collect'):
            raise NotImplementedError("pardox_lazy_collect not found in Core.")
        ptr = lib.pardox_lazy_collect(self._ptr)
        if not ptr:
            raise RuntimeError("lazy.collect() returned null. Check pipeline steps.")
        self._freed = True  # collect consumes the lazy frame
        return DataFrame(ptr)

    # =========================================================================
    # INSPECTION (without executing)
    # =========================================================================

    def describe(self):
        """
        Return a human-readable description of the query plan (as JSON string).
        Does NOT execute the pipeline.

        Returns:
            str: JSON string describing the plan.
        """
        if not hasattr(lib, 'pardox_lazy_describe'):
            raise NotImplementedError("pardox_lazy_describe not found in Core.")
        ptr = lib.pardox_lazy_describe(self._ptr)
        if not ptr:
            return "{}"
        import ctypes as _ct
        raw = _ct.cast(ptr, c_char_p).value
        return raw.decode('utf-8') if raw else "{}"

    def stats(self):
        """
        Return statistics about the lazy plan (estimated rows, steps, etc.).

        Returns:
            str: JSON string with plan statistics.
        """
        if not hasattr(lib, 'pardox_lazy_stats'):
            raise NotImplementedError("pardox_lazy_stats not found in Core.")
        ptr = lib.pardox_lazy_stats(self._ptr)
        if not ptr:
            return "{}"
        import ctypes as _ct
        raw = _ct.cast(ptr, c_char_p).value
        return raw.decode('utf-8') if raw else "{}"

    def __repr__(self):
        return f"<PardoX LazyFrame — call .collect() to execute>"

    def __del__(self):
        if not self._freed and self._ptr:
            try:
                if hasattr(lib, 'pardox_lazy_free'):
                    lib.pardox_lazy_free(self._ptr)
            except Exception:
                pass
