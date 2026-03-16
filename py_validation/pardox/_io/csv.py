import json
import os
from ..wrapper import lib

DEFAULT_CSV_CONFIG = {
    "delimiter": 44,        # Comma (,)
    "quote_char": 34,       # Double Quote (")
    "has_header": True,
    "chunk_size": 16 * 1024 * 1024  # 16MB Chunk
}


def read_csv(path, schema=None):
    """
    Reads a CSV file directly into PardoX using the native Rust engine.

    Args:
        path (str): Path to the CSV file.
        schema (dict, optional): Manual schema definition.
    """
    from ..frame import DataFrame
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    path_bytes = path.encode('utf-8')
    config_bytes = json.dumps(DEFAULT_CSV_CONFIG).encode('utf-8')

    if schema:
        cols = [{"name": k, "type": v} for k, v in schema.items()]
        schema_json_str = json.dumps({"columns": cols})
    else:
        schema_json_str = "{}"

    schema_bytes = schema_json_str.encode('utf-8')

    manager_ptr = lib.pardox_load_manager_csv(path_bytes, schema_bytes, config_bytes)

    if not manager_ptr:
        raise RuntimeError(f"Failed to load CSV: {path}.")

    return DataFrame(manager_ptr)
