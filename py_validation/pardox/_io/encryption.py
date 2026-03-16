import os
from ..wrapper import lib


def read_prdx_encrypted(path: str, key: str):
    """
    Read an encrypted .prdx file into a DataFrame.

    Example:
        df = pdx.read_prdx_encrypted("secure_data.prdx.enc", key="my-secret-key")
        print(df)
    """
    from ..frame import DataFrame
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if not hasattr(lib, 'pardox_read_prdx_encrypted'):
        raise NotImplementedError("pardox_read_prdx_encrypted not found in Core.")
    ptr = lib.pardox_read_prdx_encrypted(path.encode('utf-8'), key.encode('utf-8'))
    if not ptr:
        raise RuntimeError(f"read_prdx_encrypted failed for: {path}")
    return DataFrame(ptr)
