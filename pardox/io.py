import ctypes
import json
import os
from .wrapper import lib, c_char_p
from .frame import DataFrame

# Default configuration for Rust CSV Reader
DEFAULT_CSV_CONFIG = {
    "delimiter": 44,        # Comma (,)
    "quote_char": 34,       # Double Quote (")
    "has_header": True,
    "chunk_size": 16 * 1024 * 1024  # 16MB Chunk
}

# =============================================================================
# ARROW C DATA INTERFACE (ABI)
# =============================================================================
class ArrowSchema(ctypes.Structure):
    _fields_ = [
        ("format", ctypes.c_char_p),
        ("name", ctypes.c_char_p),
        ("metadata", ctypes.c_char_p),
        ("flags", ctypes.c_int64),
        ("n_children", ctypes.c_int64),
        ("children", ctypes.POINTER(ctypes.c_void_p)),
        ("dictionary", ctypes.c_void_p),
        ("release", ctypes.c_void_p),
        ("private_data", ctypes.c_void_p),
    ]

class ArrowArray(ctypes.Structure):
    _fields_ = [
        ("length", ctypes.c_int64),
        ("null_count", ctypes.c_int64),
        ("offset", ctypes.c_int64),
        ("n_buffers", ctypes.c_int64),
        ("n_children", ctypes.c_int64),
        ("buffers", ctypes.POINTER(ctypes.c_void_p)),
        ("children", ctypes.POINTER(ctypes.c_void_p)),
        ("dictionary", ctypes.c_void_p),
        ("release", ctypes.c_void_p),
        ("private_data", ctypes.c_void_p),
    ]

# =============================================================================
# PUBLIC API (NATIVE INGESTION)
# =============================================================================

def read_csv(path, schema=None):
    """
    Reads a CSV file directly into PardoX using the native Rust engine.
    
    Args:
        path (str): Path to the CSV file.
        schema (dict, optional): Manual schema definition.
    """
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


def read_sql(connection_string, query):
    """
    Reads data directly from a SQL database using PardoX's NATIVE Rust drivers.
    
    This function bypasses Python completely. The Rust engine connects to the
    database, executes the query, and fills the memory buffers directly.
    
    Args:
        connection_string (str): URL (e.g., "postgresql://user:pass@localhost:5432/db")
        query (str): SQL Query (e.g., "SELECT * FROM clients")
        
    Returns:
        pardox.DataFrame: A PardoX DataFrame containing the query results.
    """
    # 1. Check if the Core has Native SQL capabilities
    if not hasattr(lib, 'pardox_scan_sql'):
        raise NotImplementedError("This PardoX Core build does not support Native SQL.")

    # 2. Encode to C-Strings (UTF-8)
    conn_bytes = connection_string.encode('utf-8')
    query_bytes = query.encode('utf-8')

    # 3. Call Rust Core (Native Driver)
    manager_ptr = lib.pardox_scan_sql(conn_bytes, query_bytes)

    if not manager_ptr:
        raise RuntimeError("SQL Query failed. Check console/stderr for Rust driver errors.")

    return DataFrame(manager_ptr)


def from_arrow(data):
    """
    Ingests an Apache Arrow Table or RecordBatch into PardoX using Zero-Copy.
    
    Use this for sources not yet supported natively (e.g., Parquet, Snowflake via Arrow).
    """
    try:
        import pyarrow as pa
    except ImportError as e:
        raise ImportError("from_arrow requires 'pyarrow' installed.") from e

    try:
        if isinstance(data, pa.Table):
            data = data.combine_chunks()
            if data.num_rows == 0:
                raise ValueError("Input Arrow Table is empty.")
            batch = data.to_batches()[0]
        elif isinstance(data, pa.RecordBatch):
            batch = data
        else:
            raise TypeError("Input must be a pyarrow.Table or pyarrow.RecordBatch.")

        if batch.num_rows == 0:
             raise ValueError("Input Arrow Batch is empty.")

        c_schema = ArrowSchema()
        c_array = ArrowArray()

        batch._export_to_c(
            ctypes.addressof(c_array), 
            ctypes.addressof(c_schema)
        )

        mgr_ptr = lib.pardox_ingest_arrow_stream(
            ctypes.byref(c_array), 
            ctypes.byref(c_schema)
        )

        if not mgr_ptr:
            raise RuntimeError("PardoX Core returned NULL pointer (Ingestion Failed).")
            
        return DataFrame(mgr_ptr)

    except Exception as e:
         raise RuntimeError(f"PardoX Arrow Ingestion Failed: {e}")


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
        
    try:
        # Decode Pointer -> String
        json_str = ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
        return json.loads(json_str)
    finally:
        # Free Rust Memory
        if hasattr(lib, 'pardox_free_string'):
            lib.pardox_free_string(json_ptr)