# pardox/__init__.py

__version__ = "0.3.4"

from .frame import DataFrame
from .series import Series
from .lazy import LazyFrame
from .io import (
    # Core I/O
    read_csv,
    read_sql,
    from_arrow,
    read_prdx,
    write_sql_prdx,
    execute_sql,
    # Gap 30: SQL Cursor API (streaming batch reads)
    query_to_results,
    sql_to_parquet,
    # SQL variants
    read_mysql,
    execute_mysql,
    read_sqlserver,
    execute_sqlserver,
    read_mongodb,
    execute_mongodb,
    # Gap 5 / 27: Lazy Pipeline
    scan_csv,
    scan_prdx,
    # Gap 12: PRDX full load
    load_prdx,
    # Gap 15: Cloud Storage
    read_cloud_csv,
    read_cloud_prdx,
    write_cloud_prdx,
    # Gap 16: Live Query
    live_query,
    LiveQuery,
    # Gap 18: Encrypted PRDX
    read_prdx_encrypted,
    # Gap 20: Time Travel
    version_read,
    version_list,
    version_delete,
    # Gap 21: Arrow Flight
    flight_read,
    flight_register,
    flight_start,
    flight_stop,
    # Gap 22: Cluster
    cluster_connect,
    ClusterConnection,
    # Gap 29: REST Connector
    read_rest,
    # File stats (direct .prdx operations without loading)
    read_prdx_schema,
    query_sql_prdx,
    prdx_column_sum,
    prdx_min,
    prdx_max,
    prdx_mean,
    prdx_count,
    prdx_groupby,
    inspect_prdx,
    # Raw buffer ingestion
    ingest_buffer,
    # Bulk CSV to PRDX
    hyper_copy,
    # File format readers
    read_parquet,
    read_dat,
    # Engine utilities
    get_quarantine_logs,
    clear_quarantine,
    system_report,
    reset,
    ping,
    engine_version,
    sqlserver_config_ok,
)
from . import wrapper as _wrapper

# Expose WASM module (Gap 17)
wasm = getattr(_wrapper, 'wasm', None)


# Server components (Gap 6) — exposed so validation scripts can use pdx.PardoXPostgresServer etc.
# Point the server engine to the same CPU library already loaded by wrapper.py.
import os as _os
_os.environ.setdefault('PARDOX_LIB_PATH', _wrapper.lib_path)

try:
    from .server.pg_server import PardoXPostgresServer
    from .server.registry  import Registry
    from .server.engine    import PardoxEngine as _PardoxEngine
    # Re-export with cleaner name
    PardoxServerEngine = _PardoxEngine
except ImportError:
    PardoXPostgresServer = None  # type: ignore
    Registry             = None  # type: ignore
    PardoxServerEngine   = None  # type: ignore


# Server component (Gap 6) — optional import
def start_server(**kwargs):
    """
    Start the PardoX PostgreSQL-compatible wire-protocol server.

    Usage:
        import pardox as pdx
        pdx.start_server()
        # or with custom ports:
        pdx.start_server(port=5235, flask_port=5001)

    Then connect with:
        psql -h 127.0.0.1 -p 5235 -U pardox_user postgres
    """
    try:
        from .server.__main__ import main as _server_main
        import sys
        args = []
        for k, v in kwargs.items():
            if isinstance(v, bool):
                if v:
                    args.append(f'--{k.replace("_", "-")}')
            else:
                args.append(f'--{k.replace("_", "-")}')
                args.append(str(v))
        args.append('--start')
        sys.argv = ['pardox-server'] + args
        _server_main()
    except ImportError as e:
        raise ImportError(
            "Server dependencies not installed. "
            "Install with: pip install flask psycopg2-binary"
        ) from e


__all__ = [
    # Core classes
    "DataFrame",
    "Series",
    "LazyFrame",
    # Core I/O
    "read_csv",
    "read_sql",
    "from_arrow",
    "read_prdx",
    "load_prdx",
    "write_sql_prdx",
    "execute_sql",
    # Gap 30: SQL Cursor API
    "query_to_results",
    "sql_to_parquet",
    # SQL variants
    "read_mysql",
    "execute_mysql",
    "read_sqlserver",
    "execute_sqlserver",
    "read_mongodb",
    "execute_mongodb",
    # Gap 5/27: Lazy Pipeline
    "scan_csv",
    "scan_prdx",
    # Gap 15: Cloud
    "read_cloud_csv",
    "read_cloud_prdx",
    "write_cloud_prdx",
    # Gap 16: Live Query
    "live_query",
    "LiveQuery",
    # Gap 18: Encryption
    "read_prdx_encrypted",
    # Gap 20: Time Travel
    "version_read",
    "version_list",
    "version_delete",
    # Gap 21: Arrow Flight
    "flight_read",
    "flight_register",
    "flight_start",
    "flight_stop",
    # Gap 22: Cluster
    "cluster_connect",
    "ClusterConnection",
    # Gap 29: REST
    "read_rest",
    # File stats (direct .prdx operations without loading)
    "read_prdx_schema",
    "query_sql_prdx",
    "prdx_column_sum",
    "prdx_min",
    "prdx_max",
    "prdx_mean",
    "prdx_count",
    "prdx_groupby",
    "inspect_prdx",
    # Raw buffer ingestion
    "ingest_buffer",
    # Bulk CSV to PRDX
    "hyper_copy",
    # File format readers
    "read_parquet",
    "read_dat",
    # Engine utilities
    "get_quarantine_logs",
    "clear_quarantine",
    "system_report",
    "reset",
    "ping",
    "engine_version",
    "sqlserver_config_ok",
    # Server
    "start_server",
    # WASM
    "wasm",
]
