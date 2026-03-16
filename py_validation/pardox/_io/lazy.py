import json
import os
from ..wrapper import lib

_DEFAULT_CSV_CONFIG = {
    "delimiter": 44,
    "quote_char": 34,
    "has_header": True,
    "chunk_size": 16 * 1024 * 1024
}


def scan_csv(path: str, schema=None):
    """
    Create a LazyFrame from a CSV file (no data loaded yet).
    Call .collect() to execute and get a DataFrame.

    Example:
        lf = pdx.scan_csv("sales.csv")
        df = lf.filter("price", "gt", 100).select(["category", "price"]).collect()
        print(df)
    """
    from ..lazy import LazyFrame
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if not hasattr(lib, 'pardox_lazy_scan_csv'):
        raise NotImplementedError("pardox_lazy_scan_csv not found in Core.")
    path_bytes = path.encode('utf-8')
    config_bytes = json.dumps(_DEFAULT_CSV_CONFIG).encode('utf-8')
    if schema:
        cols = [{"name": k, "type": v} for k, v in schema.items()]
        schema_bytes = json.dumps({"columns": cols}).encode('utf-8')
    else:
        schema_bytes = b"{}"
    ptr = lib.pardox_lazy_scan_csv(path_bytes, schema_bytes, config_bytes)
    return LazyFrame(ptr)


def scan_prdx(path: str):
    """
    Create a LazyFrame from a .prdx file (no data loaded yet).

    Example:
        lf = pdx.scan_prdx("ventas_150m.prdx")
        df = lf.filter("price", "gt", 50).limit(10000).collect()
        print(df)
    """
    from ..lazy import LazyFrame
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if not hasattr(lib, 'pardox_lazy_scan_prdx'):
        raise NotImplementedError("pardox_lazy_scan_prdx not found in Core.")
    ptr = lib.pardox_lazy_scan_prdx(path.encode('utf-8'))
    return LazyFrame(ptr)
