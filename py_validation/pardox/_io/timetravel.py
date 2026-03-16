import ctypes
import json
from ..wrapper import lib, c_char_p


def version_read(path: str, label: str):
    """
    Read a versioned snapshot from disk.

    Example:
        df = pdx.version_read("/tmp/snapshots", "v1")
        print(df)
    """
    from ..frame import DataFrame
    if not hasattr(lib, 'pardox_version_read'):
        raise NotImplementedError("pardox_version_read not found in Core.")
    ptr = lib.pardox_version_read(path.encode('utf-8'), label.encode('utf-8'))
    if not ptr:
        raise RuntimeError(f"version_read failed for path={path!r}, label={label!r}")
    return DataFrame(ptr)


def version_list(path: str) -> list:
    """List all version labels for a given base path."""
    if not hasattr(lib, 'pardox_version_list'):
        raise NotImplementedError("pardox_version_list not found in Core.")
    ptr = lib.pardox_version_list(path.encode('utf-8'))
    if not ptr:
        return []
    import ctypes as _ct
    raw = _ct.cast(ptr, c_char_p).value
    return json.loads(raw.decode('utf-8')) if raw else []


def version_delete(path: str, label: str) -> int:
    """Delete a specific version snapshot."""
    if not hasattr(lib, 'pardox_version_delete'):
        raise NotImplementedError("pardox_version_delete not found in Core.")
    result = lib.pardox_version_delete(path.encode('utf-8'), label.encode('utf-8'))
    if result < 0:
        raise RuntimeError(f"version_delete failed with code: {result}")
    return int(result)
