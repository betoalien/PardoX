from ..wrapper import lib


def read_mysql(connection_string: str, query: str):
    """
    Reads data from a MySQL database using PardoX's native Rust driver.

    Args:
        connection_string (str): MySQL URL (e.g. "mysql://user:pass@host:3306/db")
        query (str): SQL query string.

    Returns:
        pardox.DataFrame
    """
    from ..frame import DataFrame
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
    from ..frame import DataFrame
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
    from ..frame import DataFrame
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


def sqlserver_config_ok() -> bool:
    """
    Check whether the SQL Server native driver is correctly configured.

    Returns:
        bool: True if configured, False otherwise.
    """
    if not hasattr(lib, 'pardox_sqlserver_config_ok'):
        raise NotImplementedError("pardox_sqlserver_config_ok not found in Core.")
    return bool(lib.pardox_sqlserver_config_ok())
