def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    if hasattr(lib, 'pardox_sql_query'):
        lib.pardox_sql_query.argtypes = [c_void_p, c_char_p]
        lib.pardox_sql_query.restype = c_void_p

    if hasattr(lib, 'pardox_execute_sql'):
        lib.pardox_execute_sql.argtypes = [c_char_p, c_char_p]
        lib.pardox_execute_sql.restype = c_longlong

    if hasattr(lib, 'pardox_write_sql'):
        lib.pardox_write_sql.argtypes = [c_void_p, c_char_p, c_char_p, c_char_p, c_char_p]
        lib.pardox_write_sql.restype = c_longlong

    if hasattr(lib, 'pardox_write_sql_prdx'):
        lib.pardox_write_sql_prdx.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p, c_char_p, c_longlong]
        lib.pardox_write_sql_prdx.restype = c_longlong

    if hasattr(lib, 'pardox_sqlserver_config_ok'):
        lib.pardox_sqlserver_config_ok.argtypes = [c_char_p]
        lib.pardox_sqlserver_config_ok.restype = c_int32

    # Gap 30 — SQL Cursor API (streaming batch reads from SQL databases)
    if hasattr(lib, 'pardox_scan_sql_cursor_open'):
        lib.pardox_scan_sql_cursor_open.argtypes = [c_char_p, c_char_p, c_size_t]
        lib.pardox_scan_sql_cursor_open.restype = c_void_p

    if hasattr(lib, 'pardox_scan_sql_cursor_fetch'):
        lib.pardox_scan_sql_cursor_fetch.argtypes = [c_void_p]
        lib.pardox_scan_sql_cursor_fetch.restype = c_void_p

    if hasattr(lib, 'pardox_scan_sql_cursor_offset'):
        lib.pardox_scan_sql_cursor_offset.argtypes = [c_void_p]
        lib.pardox_scan_sql_cursor_offset.restype = c_size_t

    if hasattr(lib, 'pardox_scan_sql_cursor_close'):
        lib.pardox_scan_sql_cursor_close.argtypes = [c_void_p]
        lib.pardox_scan_sql_cursor_close.restype = None

    if hasattr(lib, 'pardox_scan_sql_to_parquet'):
        lib.pardox_scan_sql_to_parquet.argtypes = [c_char_p, c_char_p, c_char_p, c_longlong]
        lib.pardox_scan_sql_to_parquet.restype = c_longlong
