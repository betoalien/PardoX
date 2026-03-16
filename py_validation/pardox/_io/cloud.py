import json
from ..wrapper import lib


def read_cloud_csv(uri: str, config: dict = None):
    """
    Read a CSV from cloud storage (S3, GCS, Azure Blob) into a DataFrame.

    Example:
        df = pdx.read_cloud_csv("s3://my-bucket/sales.csv")
        print(df)
    """
    from ..frame import DataFrame
    if not hasattr(lib, 'pardox_cloud_read_csv'):
        raise NotImplementedError("pardox_cloud_read_csv not found in Core.")
    config_json = json.dumps(config or {}).encode('utf-8')
    ptr = lib.pardox_cloud_read_csv(uri.encode('utf-8'), config_json)
    if not ptr:
        raise RuntimeError(f"read_cloud_csv failed for URI: {uri}")
    return DataFrame(ptr)


def read_cloud_prdx(uri: str):
    """Read a .prdx file from cloud storage into a DataFrame."""
    from ..frame import DataFrame
    if not hasattr(lib, 'pardox_cloud_read_prdx'):
        raise NotImplementedError("pardox_cloud_read_prdx not found in Core.")
    ptr = lib.pardox_cloud_read_prdx(uri.encode('utf-8'))
    if not ptr:
        raise RuntimeError(f"read_cloud_prdx failed for URI: {uri}")
    return DataFrame(ptr)


def write_cloud_prdx(df, uri: str) -> int:
    """Write a DataFrame to a .prdx file on cloud storage."""
    if not hasattr(lib, 'pardox_cloud_write_prdx'):
        raise NotImplementedError("pardox_cloud_write_prdx not found in Core.")
    result = lib.pardox_cloud_write_prdx(df._ptr, uri.encode('utf-8'))
    if result < 0:
        raise RuntimeError(f"write_cloud_prdx failed with code: {result}")
    return int(result)
