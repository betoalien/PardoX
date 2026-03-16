import ctypes
import json
import os
from ..wrapper import lib, c_char_p


def read_prdx(path, limit=100):
    """
    Reads a native PardoX (.prdx) file.

    NOTE: Currently in V0.1 Beta Showcase Mode.
    This uses the Native Reader to inspect the file structure and data integrity.
    It returns a list of dictionaries (JSON equivalent) for preview purposes.

    Args:
        path (str): Path to the .prdx file.
        limit (int): Number of rows to inspect (Head).

    Returns:
        list: A list of dicts containing the data rows.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    path_bytes = path.encode('utf-8')

    if not hasattr(lib, 'pardox_read_head_json'):
        raise NotImplementedError("API 'pardox_read_head_json' not found in Core.")

    # Call Rust Native Reader
    json_ptr = lib.pardox_read_head_json(path_bytes, limit)

    if not json_ptr:
        raise RuntimeError("Failed to read PRDX file (Rust returned NULL).")

    # pardox_read_head_json uses a thread-local buffer — do NOT call pardox_free_string
    json_str = ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
    return json.loads(json_str)


def load_prdx(path: str, limit: int = 0):
    """
    Load a .prdx file into a full in-memory DataFrame.

    Args:
        path (str): Path to the .prdx file.
        limit (int): Maximum rows to load (0 = all rows).

    Example:
        df = pdx.load_prdx("ventas_150m.prdx")
        print(df.shape)
    """
    import ctypes as _ct
    from ..frame import DataFrame
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if not hasattr(lib, 'pardox_load_manager_prdx'):
        raise NotImplementedError("pardox_load_manager_prdx not found in Core.")
    ptr = lib.pardox_load_manager_prdx(path.encode('utf-8'), _ct.c_longlong(limit))
    if not ptr:
        raise RuntimeError(f"Failed to load PRDX file: {path}")
    return DataFrame(ptr)


def read_prdx_schema(path: str) -> dict:
    """Return the schema of a .prdx file as a dict without loading data."""
    import ctypes as _ct
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if not hasattr(lib, 'pardox_read_schema'):
        raise NotImplementedError("pardox_read_schema not found in Core.")
    ptr = lib.pardox_read_schema(path.encode('utf-8'))
    if not ptr:
        raise RuntimeError(f"read_prdx_schema failed for: {path}")
    raw = _ct.cast(ptr, c_char_p).value
    return json.loads(raw.decode('utf-8')) if raw else {}


def query_sql_prdx(path: str, sql: str) -> list:
    """
    Run a SQL query directly against a .prdx file (no loading into memory).

    Returns:
        list: List of dicts (JSON records).
    """
    import ctypes as _ct
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if not hasattr(lib, 'pardox_query_sql'):
        raise NotImplementedError("pardox_query_sql not found in Core.")
    ptr = lib.pardox_query_sql(path.encode('utf-8'), sql.encode('utf-8'))
    if not ptr:
        raise RuntimeError(f"query_sql_prdx failed for: {path}")
    raw = _ct.cast(ptr, c_char_p).value
    return json.loads(raw.decode('utf-8')) if raw else []


def prdx_column_sum(path: str, col: str) -> float:
    """Sum a column directly from a .prdx file without loading it into memory."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if not hasattr(lib, 'pardox_column_sum'):
        raise NotImplementedError("pardox_column_sum not found in Core.")
    return float(lib.pardox_column_sum(path.encode('utf-8'), col.encode('utf-8')))


def prdx_min(path: str, col: str) -> float:
    """Return the minimum value of a column directly from a .prdx file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if not hasattr(lib, 'pardox_prdx_min'):
        raise NotImplementedError("pardox_prdx_min not found in Core.")
    return float(lib.pardox_prdx_min(path.encode('utf-8'), col.encode('utf-8')))


def prdx_max(path: str, col: str) -> float:
    """Return the maximum value of a column directly from a .prdx file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if not hasattr(lib, 'pardox_prdx_max'):
        raise NotImplementedError("pardox_prdx_max not found in Core.")
    return float(lib.pardox_prdx_max(path.encode('utf-8'), col.encode('utf-8')))


def prdx_mean(path: str, col: str) -> float:
    """Return the mean value of a column directly from a .prdx file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if not hasattr(lib, 'pardox_prdx_mean'):
        raise NotImplementedError("pardox_prdx_mean not found in Core.")
    return float(lib.pardox_prdx_mean(path.encode('utf-8'), col.encode('utf-8')))


def prdx_count(path: str) -> int:
    """Return the row count of a .prdx file directly (no memory load)."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if not hasattr(lib, 'pardox_prdx_count'):
        raise NotImplementedError("pardox_prdx_count not found in Core.")
    return int(lib.pardox_prdx_count(path.encode('utf-8')))


def prdx_groupby(path: str, group_cols: list, agg: dict):
    """
    Run a GroupBy aggregation directly on a .prdx file without loading it.

    Args:
        path (str): Path to the .prdx file.
        group_cols (list): Column names to group by.
        agg (dict): Aggregation spec, e.g. {"amount": "sum", "count": "count"}.

    Returns:
        pardox.DataFrame
    """
    from ..frame import DataFrame
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if not hasattr(lib, 'pardox_groupby_agg_prdx'):
        raise NotImplementedError("pardox_groupby_agg_prdx not found in Core.")
    group_bytes = json.dumps(group_cols).encode('utf-8')
    agg_bytes   = json.dumps(agg).encode('utf-8')
    ptr = lib.pardox_groupby_agg_prdx(path.encode('utf-8'), group_bytes, agg_bytes)
    if not ptr:
        raise RuntimeError(f"prdx_groupby failed for: {path}")
    return DataFrame(ptr)


def inspect_prdx(path: str, options: dict = None) -> dict:
    """
    Lazily inspect a .prdx file and return metadata (schema, stats, row count, etc.)
    without fully loading the file into memory.

    Args:
        path (str): Path to the .prdx file.
        options (dict): Optional inspection options (e.g. {"sample_rows": 100}).

    Returns:
        dict: File metadata.
    """
    import ctypes as _ct
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if not hasattr(lib, 'pardox_inspect_file_lazy'):
        raise NotImplementedError("pardox_inspect_file_lazy not found in Core.")
    opts_json = json.dumps(options or {}).encode('utf-8')
    ptr = lib.pardox_inspect_file_lazy(path.encode('utf-8'), opts_json)
    if not ptr:
        raise RuntimeError(f"inspect_prdx failed for: {path}")
    raw = _ct.cast(ptr, c_char_p).value
    return json.loads(raw.decode('utf-8')) if raw else {}
