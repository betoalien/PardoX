import json
import os
from ..wrapper import lib


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
    from ..frame import DataFrame
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


def write_sql_prdx(prdx_path, connection_string, table_name, mode="append", conflict_cols=None, batch_rows=1000000):
    """
    Write a native PardoX (.prdx) file to PostgreSQL.

    Routes automatically:
    - If pardox_write_sql_prdx is compiled in Core: uses the native streaming path.
    - Otherwise: loads the PRDX file into a HyperBlockManager via
      pardox_load_manager_prdx, then calls pardox_write_sql which internally
      uses PostgreSQL COPY FROM STDIN for datasets > 10,000 rows — the
      fastest bulk-insert path available.

    Args:
        prdx_path (str): Path to the .prdx file.
        connection_string (str): PostgreSQL connection URL.
        table_name (str): Target table name.
        mode (str): Write mode - "append" or "upsert". Default: "append".
        conflict_cols (list, optional): Columns for ON CONFLICT clause (upsert only).
        batch_rows (int): Unused in COPY path; kept for API compatibility.

    Returns:
        int: Number of rows written.
    """
    if not os.path.exists(prdx_path):
        raise FileNotFoundError(f"PRDX file not found: {prdx_path}")

    # ── Fast path: native pardox_write_sql_prdx if compiled ───────────────────
    if hasattr(lib, 'pardox_write_sql_prdx'):
        path_bytes     = prdx_path.encode('utf-8')
        conn_bytes     = connection_string.encode('utf-8')
        table_bytes    = table_name.encode('utf-8')
        mode_bytes     = mode.encode('utf-8')
        conflict_json  = json.dumps(conflict_cols or []).encode('utf-8')

        rows = lib.pardox_write_sql_prdx(
            path_bytes, conn_bytes, table_bytes, mode_bytes, conflict_json, batch_rows
        )

        if rows < 0:
            error_map = {
                -1: "Invalid PRDX path",
                -2: "Invalid connection string",
                -3: "Invalid table name",
                -4: "Invalid mode",
                -5: "Invalid conflict columns JSON",
                -10: "Failed to open PRDX file",
                -20: "PostgreSQL connection failed",
            }
            msg = error_map.get(int(rows), f"Unknown error code: {rows}")
            raise RuntimeError(f"write_sql_prdx failed: {msg}")

        return int(rows)

    # ── Fallback: load PRDX → pardox_write_sql (auto-COPY for > 10k rows) ─────
    if not hasattr(lib, 'pardox_load_manager_prdx'):
        raise NotImplementedError("API 'pardox_load_manager_prdx' not found in Core.")
    if not hasattr(lib, 'pardox_write_sql'):
        raise NotImplementedError("API 'pardox_write_sql' not found in Core.")

    # Load the entire PRDX file (-1 = no row limit)
    mgr = lib.pardox_load_manager_prdx(prdx_path.encode('utf-8'), -1)
    if not mgr:
        raise RuntimeError(f"pardox_load_manager_prdx returned null for: {prdx_path}")

    conn_bytes    = connection_string.encode('utf-8')
    table_bytes   = table_name.encode('utf-8')
    mode_bytes    = mode.encode('utf-8')
    conflict_json = json.dumps(conflict_cols or []).encode('utf-8')

    # write_postgres in Rust routes automatically:
    #   mode=="append" AND rows > 10,000  →  COPY FROM STDIN  (bulk-insert)
    #   otherwise                         →  batched INSERT
    rows = lib.pardox_write_sql(mgr, conn_bytes, table_bytes, mode_bytes, conflict_json)

    if rows < 0:
        raise RuntimeError(f"pardox_write_sql failed with code {rows}")

    # PRDX managers loaded via pardox_load_manager_prdx must be freed
    if hasattr(lib, 'pardox_free_manager'):
        lib.pardox_free_manager(mgr)

    return int(rows)
