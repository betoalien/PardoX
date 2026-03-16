import os
from ..wrapper import lib


def read_parquet(path: str):
    """
    Load a Parquet file into a PardoX DataFrame.

    Args:
        path (str): Path to the .parquet file.

    Returns:
        pardox.DataFrame
    """
    from ..frame import DataFrame
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if not hasattr(lib, 'pardox_load_manager_parquet'):
        raise NotImplementedError("pardox_load_manager_parquet not found in Core.")
    ptr = lib.pardox_load_manager_parquet(path.encode('utf-8'))
    if not ptr:
        raise RuntimeError(f"read_parquet failed for: {path}")
    return DataFrame(ptr)
