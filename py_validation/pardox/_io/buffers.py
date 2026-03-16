import json
import os
import ctypes
from ..wrapper import lib, c_char_p

_DEFAULT_CSV_CONFIG = {
    "delimiter": 44,
    "quote_char": 34,
    "has_header": True,
    "chunk_size": 16 * 1024 * 1024
}


def ingest_buffer(buffer_bytes: bytes, layout_json: dict = None, schema_json: dict = None):
    """
    Ingest raw binary buffer data directly into PardoX.

    Args:
        buffer_bytes (bytes): Raw binary data.
        layout_json (dict): Layout descriptor (e.g. {"format": "columnar"}).
        schema_json (dict): Schema descriptor.

    Returns:
        pardox.DataFrame
    """
    import ctypes as _ct
    from ..frame import DataFrame
    if not hasattr(lib, 'pardox_ingest_buffer'):
        raise NotImplementedError("pardox_ingest_buffer not found in Core.")
    buf_ptr   = _ct.cast(_ct.c_char_p(buffer_bytes), _ct.c_void_p)
    layout_b  = json.dumps(layout_json or {}).encode('utf-8')
    schema_b  = json.dumps(schema_json or {}).encode('utf-8')
    ptr = lib.pardox_ingest_buffer(buf_ptr, len(buffer_bytes), layout_b, schema_b)
    if not ptr:
        raise RuntimeError("ingest_buffer returned null. Check buffer format and schema.")
    return DataFrame(ptr)


def hyper_copy(src_pattern: str, out_path: str, schema: dict = None, config: dict = None) -> int:
    """
    Bulk-convert CSV files matching a glob pattern to a single .prdx file.
    This is the native COPY mechanism — much faster than loading CSV row-by-row.

    Args:
        src_pattern (str): Glob pattern for source CSV files (e.g. "/data/*.csv").
        out_path (str): Destination .prdx file path.
        schema (dict): Optional schema override.
        config (dict): Optional CSV config (delimiter, quote_char, etc.).

    Returns:
        int: Number of rows copied.

    Example:
        rows = pdx.hyper_copy("/data/sales_*.csv", "/data/sales_all.prdx")
        print(f"Copied {rows} rows")
    """
    if not hasattr(lib, 'pardox_hyper_copy_v3'):
        raise NotImplementedError("pardox_hyper_copy_v3 not found in Core.")
    schema_bytes = json.dumps(schema or {}).encode('utf-8')
    config_bytes = json.dumps(config or _DEFAULT_CSV_CONFIG).encode('utf-8')
    result = lib.pardox_hyper_copy_v3(
        src_pattern.encode('utf-8'),
        out_path.encode('utf-8'),
        schema_bytes,
        config_bytes,
    )
    if result < 0:
        raise RuntimeError(f"hyper_copy failed with code: {result}")
    return int(result)


def read_dat(path: str, schema_json: dict = None):
    """
    Import a mainframe fixed-width .dat file into a PardoX DataFrame.

    Args:
        path (str): Path to the .dat file.
        schema_json (dict): Schema descriptor for fixed-width fields.

    Returns:
        pardox.DataFrame
    """
    from ..frame import DataFrame
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if not hasattr(lib, 'pardox_import_mainframe_dat'):
        raise NotImplementedError("pardox_import_mainframe_dat not found in Core.")
    schema_bytes = json.dumps(schema_json or {}).encode('utf-8')
    ptr = lib.pardox_import_mainframe_dat(path.encode('utf-8'), schema_bytes)
    if not ptr:
        raise RuntimeError(f"read_dat failed for: {path}")
    return DataFrame(ptr)
