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
        
    # pardox_read_head_json uses a thread-local buffer — do NOT call pardox_free_string
    json_str = ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
    return json.loads(json_str)


def read_mysql(connection_string: str, query: str):
    """
    Reads data from a MySQL database using PardoX's native Rust driver.

    Args:
        connection_string (str): MySQL URL (e.g. "mysql://user:pass@host:3306/db")
        query (str): SQL query string.

    Returns:
        pardox.DataFrame
    """
    if not hasattr(lib, 'pardox_read_mysql'):
        raise NotImplementedError("API 'pardox_read_mysql' not found in Core. Re-compile Rust.")
    conn_bytes  = connection_string.encode('utf-8')
    query_bytes = query.encode('utf-8')
    manager_ptr = lib.pardox_read_mysql(conn_bytes, query_bytes)
    if not manager_ptr:
        raise RuntimeError("MySQL query failed. Check console/stderr for Rust driver errors.")
    return DataFrame(manager_ptr)


def execute_mysql(connection_string: str, query: str) -> int:
    """
    Execute arbitrary DDL or DML directly against MySQL via the Rust native driver.

    Args:
        connection_string (str): MySQL URL.
        query (str): SQL statement to execute.

    Returns:
        int: Number of rows affected.
    """
    if not hasattr(lib, 'pardox_execute_mysql'):
        raise NotImplementedError("API 'pardox_execute_mysql' not found in Core. Re-compile Rust.")
    conn_bytes  = connection_string.encode('utf-8')
    query_bytes = query.encode('utf-8')
    rows = lib.pardox_execute_mysql(conn_bytes, query_bytes)
    if rows < 0:
        raise RuntimeError(f"execute_mysql failed with code: {rows}")
    return int(rows)


def read_sqlserver(connection_string: str, query: str):
    """
    Reads data from SQL Server using PardoX's native Rust driver (tiberius).

    Args:
        connection_string (str): ADO.NET connection string.
            e.g. "Server=localhost,1433;Database=mydb;User Id=sa;Password=pwd;TrustServerCertificate=true"
        query (str): SQL query string.

    Returns:
        pardox.DataFrame
    """
    if not hasattr(lib, 'pardox_read_sqlserver'):
        raise NotImplementedError("API 'pardox_read_sqlserver' not found in Core. Re-compile Rust.")
    conn_bytes  = connection_string.encode('utf-8')
    query_bytes = query.encode('utf-8')
    manager_ptr = lib.pardox_read_sqlserver(conn_bytes, query_bytes)
    if not manager_ptr:
        raise RuntimeError("SQL Server query failed. Check console/stderr for Rust driver errors.")
    return DataFrame(manager_ptr)


def execute_sqlserver(connection_string: str, query: str) -> int:
    """
    Execute arbitrary DDL or DML directly against SQL Server via the Rust native driver.

    Args:
        connection_string (str): ADO.NET connection string.
        query (str): SQL statement to execute.

    Returns:
        int: Number of rows affected.
    """
    if not hasattr(lib, 'pardox_execute_sqlserver'):
        raise NotImplementedError("API 'pardox_execute_sqlserver' not found in Core. Re-compile Rust.")
    conn_bytes  = connection_string.encode('utf-8')
    query_bytes = query.encode('utf-8')
    rows = lib.pardox_execute_sqlserver(conn_bytes, query_bytes)
    if rows < 0:
        raise RuntimeError(f"execute_sqlserver failed with code: {rows}")
    return int(rows)


def read_mongodb(connection_string: str, db_dot_collection: str):
    """
    Reads a MongoDB collection into a PardoX DataFrame using the native Rust driver.

    Args:
        connection_string (str): MongoDB URI (e.g. "mongodb://user:pass@host:27017")
        db_dot_collection (str): Target as "database.collection" (e.g. "mydb.orders")

    Returns:
        pardox.DataFrame
    """
    if not hasattr(lib, 'pardox_read_mongodb'):
        raise NotImplementedError("API 'pardox_read_mongodb' not found in Core. Re-compile Rust.")
    conn_bytes   = connection_string.encode('utf-8')
    target_bytes = db_dot_collection.encode('utf-8')
    manager_ptr = lib.pardox_read_mongodb(conn_bytes, target_bytes)
    if not manager_ptr:
        raise RuntimeError("MongoDB read failed. Check console/stderr for Rust driver errors.")
    return DataFrame(manager_ptr)


def execute_mongodb(connection_string: str, database: str, command_json: str) -> int:
    """
    Execute a MongoDB command (e.g. drop, create, custom commands) via the Rust native driver.

    Args:
        connection_string (str): MongoDB URI.
        database (str): Target database name.
        command_json (str): JSON string of the MongoDB command document.
            e.g. '{"drop": "my_collection"}'

    Returns:
        int: Value of the 'n' or 'ok' field from the command result.
    """
    if not hasattr(lib, 'pardox_execute_mongodb'):
        raise NotImplementedError("API 'pardox_execute_mongodb' not found in Core. Re-compile Rust.")
    conn_bytes = connection_string.encode('utf-8')
    db_bytes   = database.encode('utf-8')
    cmd_bytes  = command_json.encode('utf-8')
    result = lib.pardox_execute_mongodb(conn_bytes, db_bytes, cmd_bytes)
    if result < 0:
        raise RuntimeError(f"execute_mongodb failed with code: {result}")
    return int(result)


def execute_sql(connection_string: str, query: str) -> int:
    """
    Execute arbitrary DDL or DML directly via the Rust native driver.

    Bypasses Python completely: Rust connects to the database and runs the
    statement. Useful for CREATE TABLE, DROP TABLE, UPDATE, DELETE, etc.

    Args:
        connection_string (str): PostgreSQL URL (e.g. "postgresql://user:pass@host/db")
        query (str): SQL statement to execute.

    Returns:
        int: Number of rows affected (0 for DDL statements).
    """
    if not hasattr(lib, 'pardox_execute_sql'):
        raise NotImplementedError("API 'pardox_execute_sql' not found in Core. Re-compile Rust.")

    conn_bytes  = connection_string.encode('utf-8')
    query_bytes = query.encode('utf-8')

    rows = lib.pardox_execute_sql(conn_bytes, query_bytes)

    if rows < 0:
        error_map = {
            -1:   "Invalid connection string",
            -2:   "Invalid query string",
            -100: "Execute operation failed (check connection and SQL syntax)",
        }
        msg = error_map.get(int(rows), f"Unknown error code: {rows}")
        raise RuntimeError(f"execute_sql failed: {msg}")

    return int(rows)