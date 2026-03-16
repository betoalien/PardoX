def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    # pardox_load_manager_csv
    if hasattr(lib, 'pardox_load_manager_csv'):
        lib.pardox_load_manager_csv.argtypes = [c_char_p, c_char_p, c_char_p]
        lib.pardox_load_manager_csv.restype = c_void_p

    # pardox_scan_sql
    try:
        if hasattr(lib, 'pardox_scan_sql'):
            lib.pardox_scan_sql.argtypes = [c_char_p, c_char_p]
            lib.pardox_scan_sql.restype = c_void_p
    except AttributeError:
        pass

    # pardox_hash_join
    if hasattr(lib, 'pardox_hash_join'):
        lib.pardox_hash_join.argtypes = [c_void_p, c_void_p, c_char_p, c_char_p]
        lib.pardox_hash_join.restype = c_void_p

    # pardox_free_manager
    if hasattr(lib, 'pardox_free_manager'):
        lib.pardox_free_manager.argtypes = [c_void_p]
        lib.pardox_free_manager.restype = None

    # pardox_free_string
    try:
        if hasattr(lib, 'pardox_free_string'):
            lib.pardox_free_string.argtypes = [c_void_p]
            lib.pardox_free_string.restype = None
    except AttributeError:
        pass

    # pardox_ingest_arrow_stream
    try:
        if hasattr(lib, 'pardox_ingest_arrow_stream'):
            lib.pardox_ingest_arrow_stream.argtypes = [c_void_p, c_void_p]
            lib.pardox_ingest_arrow_stream.restype = c_void_p
    except AttributeError:
        pass

    # pardox_init_engine (binding only — call stays in wrapper.py)
    try:
        if hasattr(lib, 'pardox_init_engine'):
            lib.pardox_init_engine.argtypes = []
            lib.pardox_init_engine.restype = None
    except AttributeError:
        pass
