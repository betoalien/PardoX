import ctypes
import json
from ..wrapper import lib, c_char_p


def get_quarantine_logs() -> list:
    """
    Return all quarantine log entries from the PardoX engine.
    Quarantine logs record rows that were rejected during ingestion due to type errors.

    Returns:
        list: List of quarantine log dicts.
    """
    import ctypes as _ct
    if not hasattr(lib, 'pardox_get_quarantine_logs'):
        raise NotImplementedError("pardox_get_quarantine_logs not found in Core.")
    ptr = lib.pardox_get_quarantine_logs()
    if not ptr:
        return []
    raw = _ct.cast(ptr, c_char_p).value
    return json.loads(raw.decode('utf-8')) if raw else []


def clear_quarantine() -> None:
    """Clear all quarantine log entries from the PardoX engine."""
    if not hasattr(lib, 'pardox_clear_quarantine'):
        raise NotImplementedError("pardox_clear_quarantine not found in Core.")
    lib.pardox_clear_quarantine()


def system_report() -> dict:
    """
    Return a system report from the PardoX engine (CPU, memory, version info).

    Returns:
        dict: System information dictionary.
    """
    import ctypes as _ct
    if not hasattr(lib, 'pardox_get_system_report'):
        raise NotImplementedError("pardox_get_system_report not found in Core.")
    ptr = lib.pardox_get_system_report()
    if not ptr:
        return {}
    raw = _ct.cast(ptr, c_char_p).value
    return json.loads(raw.decode('utf-8')) if raw else {}


def reset() -> None:
    """Reset the PardoX global engine state (clears all in-memory data)."""
    if not hasattr(lib, 'pardox_reset'):
        raise NotImplementedError("pardox_reset not found in Core.")
    lib.pardox_reset()


def ping(system_name: str = "pardox") -> int:
    """
    Send a legacy ping to verify the PardoX engine is responding.

    Args:
        system_name (str): System identifier string.

    Returns:
        int: Ping response code (>= 0 = success).
    """
    if not hasattr(lib, 'pardox_legacy_ping'):
        raise NotImplementedError("pardox_legacy_ping not found in Core.")
    return int(lib.pardox_legacy_ping(system_name.encode('utf-8')))


def engine_version() -> str:
    """
    Return the PardoX Core library version string.

    Returns:
        str: Version string (e.g. "pardoX v0.3.2").
    """
    import ctypes as _ct
    if not hasattr(lib, 'pardox_version'):
        raise NotImplementedError("pardox_version not found in Core.")
    ptr = lib.pardox_version()
    if not ptr:
        return ""
    raw = _ct.cast(ptr, c_char_p).value
    return raw.decode('utf-8') if raw else ""
