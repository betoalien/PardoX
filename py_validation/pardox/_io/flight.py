import ctypes
from ..wrapper import lib


def flight_read(server: str, port: int, dataset: str):
    """
    Read a dataset from an Arrow Flight server.

    Example:
        df = pdx.flight_read("127.0.0.1", 8815, "sales_data")
        print(df)
    """
    import ctypes as _ct
    from ..frame import DataFrame
    if not hasattr(lib, 'pardox_flight_read'):
        raise NotImplementedError("pardox_flight_read not found in Core.")
    ptr = lib.pardox_flight_read(
        server.encode('utf-8'),
        _ct.c_uint16(port),
        dataset.encode('utf-8')
    )
    if not ptr:
        raise RuntimeError(f"flight_read failed: {server}:{port}/{dataset!r}")
    return DataFrame(ptr)


def flight_register(name: str, df) -> int:
    """Register a DataFrame on the local Arrow Flight server."""
    if not hasattr(lib, 'pardox_flight_register'):
        raise NotImplementedError("pardox_flight_register not found in Core.")
    result = lib.pardox_flight_register(name.encode('utf-8'), df._ptr)
    if result < 0:
        raise RuntimeError(f"flight_register failed with code: {result}")
    return int(result)


def flight_start(port: int = 8815) -> int:
    """Start a local Arrow Flight server."""
    import ctypes as _ct
    if not hasattr(lib, 'pardox_flight_start'):
        raise NotImplementedError("pardox_flight_start not found in Core.")
    return int(lib.pardox_flight_start(_ct.c_uint16(port)))


def flight_stop() -> int:
    """Stop the local Arrow Flight server."""
    if not hasattr(lib, 'pardox_flight_stop'):
        raise NotImplementedError("pardox_flight_stop not found in Core.")
    return int(lib.pardox_flight_stop())
