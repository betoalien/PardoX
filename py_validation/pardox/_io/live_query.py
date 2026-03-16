from ..wrapper import lib


class LiveQuery:
    """
    A handle to a live/streaming query that auto-refreshes as the source changes.

    Example:
        lq = pdx.live_query("sales.prdx", "SELECT category, SUM(price) FROM df GROUP BY category")
        df = lq.take()
        print(df)
        lq.free()
    """
    def __init__(self, ptr):
        self._ptr = ptr

    def version(self) -> int:
        if not hasattr(lib, 'pardox_live_query_version'):
            raise NotImplementedError("pardox_live_query_version not found in Core.")
        return int(lib.pardox_live_query_version(self._ptr))

    def take(self):
        from ..frame import DataFrame
        if not hasattr(lib, 'pardox_live_query_take'):
            raise NotImplementedError("pardox_live_query_take not found in Core.")
        ptr = lib.pardox_live_query_take(self._ptr)
        if not ptr:
            raise RuntimeError("live_query.take() returned null.")
        return DataFrame(ptr)

    def free(self):
        if self._ptr and hasattr(lib, 'pardox_live_query_free'):
            lib.pardox_live_query_free(self._ptr)
            self._ptr = None

    def __del__(self):
        self.free()

    def __repr__(self):
        return "<PardoX LiveQuery>"


def live_query(path: str, query: str, interval_ms: int = 5000):
    """
    Start a live query that auto-refreshes every interval_ms milliseconds.

    Example:
        lq = pdx.live_query("sales.prdx", "SELECT * FROM df LIMIT 10", interval_ms=2000)
        df = lq.take()
        print(df)
        lq.free()
    """
    import ctypes as _ct
    if not hasattr(lib, 'pardox_live_query_start'):
        raise NotImplementedError("pardox_live_query_start not found in Core.")
    ptr = lib.pardox_live_query_start(
        path.encode('utf-8'),
        query.encode('utf-8'),
        _ct.c_longlong(interval_ms)
    )
    if not ptr:
        raise RuntimeError(f"live_query failed for path: {path}")
    return LiveQuery(ptr)
