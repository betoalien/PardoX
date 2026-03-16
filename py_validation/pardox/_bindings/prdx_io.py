def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    if hasattr(lib, 'pardox_load_manager_prdx'):
        lib.pardox_load_manager_prdx.argtypes = [c_char_p, c_longlong]
        lib.pardox_load_manager_prdx.restype = c_void_p

    if hasattr(lib, 'pardox_to_prdx'):
        lib.pardox_to_prdx.argtypes = [c_void_p, c_char_p]
        lib.pardox_to_prdx.restype = c_longlong

    if hasattr(lib, 'pardox_load_manager_parquet'):
        lib.pardox_load_manager_parquet.argtypes = [c_char_p]
        lib.pardox_load_manager_parquet.restype = c_void_p

    if hasattr(lib, 'pardox_export_to_parquet'):
        lib.pardox_export_to_parquet.argtypes = [c_char_p, c_char_p]
        lib.pardox_export_to_parquet.restype = c_longlong

    if hasattr(lib, 'pardox_write_parquet'):
        lib.pardox_write_parquet.argtypes = [c_char_p]
        lib.pardox_write_parquet.restype = c_longlong

    try:
        if hasattr(lib, 'pardox_read_head_json'):
            lib.pardox_read_head_json.argtypes = [c_char_p, c_size_t]
            lib.pardox_read_head_json.restype = c_char_p

        if hasattr(lib, 'pardox_column_sum'):
            lib.pardox_column_sum.argtypes = [c_char_p, c_char_p]
            lib.pardox_column_sum.restype = c_double

        if hasattr(lib, 'pardox_query_sql'):
            lib.pardox_query_sql.argtypes = [c_char_p, c_char_p]
            lib.pardox_query_sql.restype = c_char_p

    except Exception:
        pass

    for fn, args, ret in [
        ('pardox_prdx_min',   [c_char_p, c_char_p], c_double),
        ('pardox_prdx_max',   [c_char_p, c_char_p], c_double),
        ('pardox_prdx_mean',  [c_char_p, c_char_p], c_double),
        ('pardox_prdx_count', [c_char_p],            c_longlong),
    ]:
        if hasattr(lib, fn):
            getattr(lib, fn).argtypes = args
            getattr(lib, fn).restype  = ret

    if hasattr(lib, 'pardox_read_schema'):
        lib.pardox_read_schema.argtypes = [c_char_p]
        lib.pardox_read_schema.restype = c_void_p

    if hasattr(lib, 'pardox_read_head'):
        lib.pardox_read_head.argtypes = [c_char_p, c_longlong]
        lib.pardox_read_head.restype = c_void_p
