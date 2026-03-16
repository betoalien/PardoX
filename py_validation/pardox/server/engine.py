from __future__ import annotations
"""
pardox_server/engine.py
Thin ctypes wrapper around pardox_cpu (libpardox.so).
Only exposes the subset of FFI needed by the PostgreSQL server.
"""
import ctypes
import json
import os
import sys


def _find_lib(explicit_path: str | None = None) -> str:
    if explicit_path and os.path.exists(explicit_path):
        return os.path.realpath(explicit_path)

    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        # Env var override
        os.environ.get("PARDOX_LIB_PATH", ""),
        # Same directory as this file (copied)
        os.path.join(here, "libpardox.so"),
        # gap_validations alongside the project
        os.path.join(here, "..", "gap_validations", "libpardox.so"),
        # Rust build output
        os.path.join(here, "..", "Pardox-Core-v0.3.2", "pardox_cpu",
                     "target", "release", "libpardox_cpu.so"),
    ]
    for p in candidates:
        if p and os.path.exists(p):
            return os.path.realpath(p)
    raise FileNotFoundError(
        "pardox_cpu library not found. Set PARDOX_LIB_PATH or copy libpardox.so "
        "into the pardox_server/ directory."
    )


class PardoxEngine:
    """Loads pardox_cpu and exposes load/query/free helpers."""

    def __init__(self, lib_path: str | None = None):
        path = _find_lib(lib_path)
        print(f"[pardox_server] Loading library: {path}")
        self._lib = ctypes.CDLL(path)
        self._setup_bindings()
        self._lib.pardox_init_engine()
        # Cache: abs_path -> (mtime, mgr_ptr) — avoids reloading 1GB+ files per query
        self._cache: dict[str, tuple[float, int]] = {}
        self._cache_lock = __import__('threading').Lock()

    def _setup_bindings(self):
        lib = self._lib

        lib.pardox_init_engine.argtypes = []
        lib.pardox_init_engine.restype  = None

        # Load .prdx (limit < 0 means all rows)
        lib.pardox_load_manager_prdx.argtypes = [ctypes.c_char_p, ctypes.c_longlong]
        lib.pardox_load_manager_prdx.restype   = ctypes.c_void_p

        # Load .parquet
        lib.pardox_load_manager_parquet.argtypes = [ctypes.c_char_p]
        lib.pardox_load_manager_parquet.restype   = ctypes.c_void_p

        # SQL query against an in-memory manager (table name inside SQL must be "t")
        lib.pardox_sql_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
        lib.pardox_sql_query.restype  = ctypes.c_void_p

        # Schema JSON: {"columns": [{"name": "...", "type": "..."}, ...]}
        lib.pardox_get_schema_json.argtypes = [ctypes.c_void_p]
        lib.pardox_get_schema_json.restype  = ctypes.c_char_p

        # Row count
        lib.pardox_get_row_count.argtypes = [ctypes.c_void_p]
        lib.pardox_get_row_count.restype  = ctypes.c_longlong

        # JSON records (heap-allocated — use c_void_p to avoid auto-free)
        lib.pardox_to_json_records.argtypes = [ctypes.c_void_p]
        lib.pardox_to_json_records.restype  = ctypes.c_void_p

        # Free a HyperBlockManager
        lib.pardox_free_manager.argtypes = [ctypes.c_void_p]
        lib.pardox_free_manager.restype  = None

        # Gap-13 streaming scalars (file-path based, no full load)
        lib.pardox_prdx_count.argtypes = [ctypes.c_char_p]
        lib.pardox_prdx_count.restype  = ctypes.c_longlong

        lib.pardox_prdx_min.argtypes  = [ctypes.c_char_p, ctypes.c_char_p]
        lib.pardox_prdx_min.restype   = ctypes.c_double
        lib.pardox_prdx_max.argtypes  = [ctypes.c_char_p, ctypes.c_char_p]
        lib.pardox_prdx_max.restype   = ctypes.c_double
        lib.pardox_prdx_mean.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        lib.pardox_prdx_mean.restype  = ctypes.c_double

        # Gap-13 streaming GROUP BY (file-path based)
        lib.pardox_groupby_agg_prdx.argtypes = [
            ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p
        ]
        lib.pardox_groupby_agg_prdx.restype = ctypes.c_void_p

        # Gap-24 INNER hash join between two in-memory managers
        lib.pardox_hash_join.argtypes = [
            ctypes.c_void_p, ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p
        ]
        lib.pardox_hash_join.restype = ctypes.c_void_p

        # Free a heap-allocated C string (may not exist in all builds)
        if hasattr(self._lib, 'pardox_free_string'):
            lib.pardox_free_string.argtypes = [ctypes.c_void_p]
            lib.pardox_free_string.restype  = None
            self._has_free_string = True
        else:
            self._has_free_string = False

    # ── public helpers ──────────────────────────────────────────────────────

    def _cached_load(self, path: str, loader_fn) -> int:
        """Load a file, returning a cached manager if the file hasn't changed."""
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            return 0
        with self._cache_lock:
            if path in self._cache:
                cached_mtime, cached_mgr = self._cache[path]
                if cached_mtime == mtime and cached_mgr:
                    return cached_mgr
            mgr = loader_fn()
            if mgr:
                self._cache[path] = (mtime, mgr)
            return mgr or 0

    # Maximum rows to load into RAM for interactive queries.
    # Files with more rows than this threshold use streaming APIs instead.
    _SAFE_LOAD_ROWS = 2_000_000   # 2M rows — safe for most machines

    def load_prdx(self, path: str) -> int:
        """Load a .prdx file (cached). Returns opaque manager pointer (int) or 0."""
        return self._cached_load(
            path,
            lambda: self._lib.pardox_load_manager_prdx(path.encode(), ctypes.c_longlong(-1)) or 0,
        )

    def load_prdx_limited(self, path: str, n_rows: int) -> int:
        """Load at most n_rows rows from a .prdx file (NOT cached — disposable load)."""
        result = self._lib.pardox_load_manager_prdx(
            path.encode(), ctypes.c_longlong(n_rows)
        )
        return result or 0

    def prdx_count(self, path: str) -> int:
        """Return the row count of a .prdx file without loading it into RAM."""
        return int(self._lib.pardox_prdx_count(path.encode()))

    def prdx_scalar(self, path: str, col: str, func: str) -> float:
        """Return a scalar aggregate (min/max/mean) for a column without full load."""
        func = func.upper()
        if func == 'MIN':
            return float(self._lib.pardox_prdx_min(path.encode(), col.encode()))
        if func == 'MAX':
            return float(self._lib.pardox_prdx_max(path.encode(), col.encode()))
        if func in ('AVG', 'MEAN'):
            return float(self._lib.pardox_prdx_mean(path.encode(), col.encode()))
        return 0.0

    def prdx_groupby(self, path: str, group_cols: list, agg_specs: list) -> int:
        """Streaming GROUP BY on a .prdx file (Gap 13 — no full load)."""
        import json
        result = self._lib.pardox_groupby_agg_prdx(
            path.encode(),
            json.dumps(group_cols).encode(),
            json.dumps(agg_specs).encode(),
        )
        return result or 0

    def prdx_row_count_approx(self, path: str) -> int:
        """Estimate row count from file size to decide load strategy."""
        try:
            size = os.path.getsize(path)
            # Rough estimate: 40 bytes/row average for prdx
            return size // 40
        except OSError:
            return 0

    def load_parquet(self, path: str) -> int:
        """Load a .parquet file (cached). Returns opaque manager pointer (int) or 0."""
        return self._cached_load(
            path,
            lambda: self._lib.pardox_load_manager_parquet(path.encode()) or 0,
        )

    def sql_query(self, mgr: int, sql: str) -> int:
        """Run SQL (table name must be 't'). Returns result manager ptr or 0."""
        result = self._lib.pardox_sql_query(mgr, sql.encode())
        return result or 0

    def get_schema(self, mgr: int) -> list[tuple[str, str]]:
        """Return [(col_name, type_str), ...].  type_str e.g. 'Int64', 'Float64', 'Utf8'."""
        raw = self._lib.pardox_get_schema_json(mgr)
        if not raw:
            return []
        data = json.loads(raw.decode())
        return [(c["name"], c["type"]) for c in data.get("columns", [])]

    def get_row_count(self, mgr: int) -> int:
        return int(self._lib.pardox_get_row_count(mgr))

    # Maximum rows to materialise as JSON in a single call.
    # Prevents OOM in pardox_to_json_records for unexpectedly large result sets.
    _MAX_JSON_ROWS = 2_000

    def get_records(self, mgr: int) -> list[dict]:
        """Return list of dicts from pardox_to_json_records.

        If the result manager contains more than _MAX_JSON_ROWS rows we run a
        second SQL pass on the result itself to cap it.  This is a last-resort
        safety net; the Rust SQL engine should already enforce LIMIT upstream.
        """
        row_count = int(self._lib.pardox_get_row_count(mgr))
        if row_count > self._MAX_JSON_ROWS:
            # Re-run a SELECT LIMIT on the result manager to cap the rows.
            capped_sql = f'SELECT * FROM t LIMIT {self._MAX_JSON_ROWS}'.encode()
            capped = self._lib.pardox_sql_query(mgr, capped_sql) or 0
            if capped:
                ptr = self._lib.pardox_to_json_records(capped)
                self._lib.pardox_free_manager(capped)
            else:
                # Fallback: serialise as-is (should never happen after Rust fixes)
                ptr = self._lib.pardox_to_json_records(mgr)
        else:
            ptr = self._lib.pardox_to_json_records(mgr)

        if not ptr:
            return []
        raw = ctypes.string_at(ptr)
        if self._has_free_string:
            self._lib.pardox_free_string(ptr)
        records = json.loads(raw)
        return records if isinstance(records, list) else []

    def hash_join(self, left_mgr: int, right_mgr: int,
                  left_key: str, right_key: str) -> int:
        """INNER hash join between two managers. Returns result manager ptr or 0."""
        result = self._lib.pardox_hash_join(
            left_mgr, right_mgr, left_key.encode(), right_key.encode()
        )
        return result or 0

    def free_manager(self, mgr: int):
        if mgr:
            self._lib.pardox_free_manager(mgr)
