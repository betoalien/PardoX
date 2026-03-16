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
