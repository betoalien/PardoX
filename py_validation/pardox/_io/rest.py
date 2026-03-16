import json
from ..wrapper import lib


def read_rest(url: str, headers: dict = None):
    """
    Fetch JSON data from a REST API endpoint into a DataFrame.
    The endpoint must return a JSON array of objects.

    Example:
        df = pdx.read_rest("https://api.example.com/sales")
        print(df)
    """
    from ..frame import DataFrame
    if not hasattr(lib, 'pardox_read_rest'):
        raise NotImplementedError("pardox_read_rest not found in Core.")
    headers_json = json.dumps(headers or {}).encode('utf-8')
    ptr = lib.pardox_read_rest(url.encode('utf-8'), headers_json)
    if not ptr:
        raise RuntimeError(f"read_rest failed for URL: {url}")
    return DataFrame(ptr)
