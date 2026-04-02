import json
import os
from typing import Iterator
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


def query_to_results(connection_string: str, query: str, batch_size: int = 100_000) -> Iterator:
    """
    Stream SQL query results in batches as an iterator of DataFrames.

    Reads large SQL tables without loading the entire result into memory.
    Only `batch_size` rows are held in RAM at any time — each DataFrame is
    yielded and should be processed before the next iteration.

    Requires the Rust core to export `pardox_scan_sql_cursor_open`,
    `pardox_scan_sql_cursor_fetch`, and `pardox_scan_sql_cursor_close`.

    Args:
        connection_string (str): PostgreSQL URL
                                 (e.g. "postgresql://user:pass@localhost:5432/db").
        query (str):             SQL query to stream
                                 (e.g. "SELECT * FROM orders").
        batch_size (int):        Number of rows per chunk. Default: 100,000.

    Yields:
        pardox.DataFrame: One DataFrame per batch. Free memory after use.

    Raises:
        NotImplementedError: If the Rust core does not export the cursor API.
        RuntimeError:        If the cursor cannot be opened.

    Example::

        import pardox as pdx

        for chunk in pdx.query_to_results(
            "postgresql://user:pass@localhost:5432/db",
            "SELECT * FROM orders",
            batch_size=50_000,
        ):
            arrow_table = chunk.to_arrow()
            # or: chunk.to_prdx(f"orders_{i}.prdx")
    """
    from ..frame import DataFrame

    if not hasattr(lib, 'pardox_scan_sql_cursor_open'):
        raise NotImplementedError(
            "The current PardoX Core build does not export 'pardox_scan_sql_cursor_open'. "
            "Re-compile the Rust core with Gap 30 (SQL Cursor API) enabled."
        )

    conn_bytes  = connection_string.encode('utf-8')
    query_bytes = query.encode('utf-8')

    cursor = lib.pardox_scan_sql_cursor_open(conn_bytes, query_bytes, batch_size)
    if not cursor:
        raise RuntimeError(
            "pardox_scan_sql_cursor_open returned null. "
            "Check the connection string and SQL syntax."
        )

    try:
        while True:
            mgr = lib.pardox_scan_sql_cursor_fetch(cursor)
            if not mgr:
                break
            yield DataFrame(mgr)
    finally:
        lib.pardox_scan_sql_cursor_close(cursor)


def sql_to_parquet(
    connection_string: str,
    query: str,
    output_pattern: str,
    chunk_size: int = 100_000,
) -> int:
    """
    Export a SQL query directly to multiple Parquet files without loading into RAM.

    Streams query results in chunks and writes each chunk as a separate Parquet
    file using the pattern provided (e.g. ``"orders_chunk_{i}.parquet"``).

    Requires the Rust core to export `pardox_scan_sql_to_parquet`.

    Args:
        connection_string (str): PostgreSQL URL.
        query (str):             SQL query to export.
        output_pattern (str):    File path pattern with ``{i}`` placeholder
                                 (e.g. ``"/data/orders_chunk_{i}.parquet"``).
        chunk_size (int):        Rows per Parquet file. Default: 100,000.

    Returns:
        int: Total rows exported.

    Raises:
        NotImplementedError: If the Rust core does not export the function.
        RuntimeError:        If the export fails.

    Example::

        import pardox as pdx

        total = pdx.sql_to_parquet(
            "postgresql://user:pass@localhost:5432/db",
            "SELECT * FROM orders",
            "/data/orders_chunk_{i}.parquet",
            chunk_size=100_000,
        )
        print(f"Exported {total:,} rows")
    """
    if not hasattr(lib, 'pardox_scan_sql_to_parquet'):
        raise NotImplementedError(
            "The current PardoX Core build does not export 'pardox_scan_sql_to_parquet'. "
            "Re-compile the Rust core with Gap 30 (SQL Cursor API) enabled."
        )

    conn_bytes    = connection_string.encode('utf-8')
    query_bytes   = query.encode('utf-8')
    pattern_bytes = output_pattern.encode('utf-8')

    total = lib.pardox_scan_sql_to_parquet(conn_bytes, query_bytes, pattern_bytes, chunk_size)

    if total < 0:
        raise RuntimeError(f"sql_to_parquet failed with error code: {total}")

    return int(total)
