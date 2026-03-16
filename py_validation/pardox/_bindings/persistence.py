def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    try:
        if hasattr(lib, 'pardox_to_csv'):
            lib.pardox_to_csv.argtypes = [c_void_p, c_char_p]
            lib.pardox_to_csv.restype = c_longlong

        if hasattr(lib, 'pardox_to_parquet'):
            lib.pardox_to_parquet.argtypes = [c_void_p, c_char_p]
            lib.pardox_to_parquet.restype = c_longlong

    except Exception:
        pass

    if hasattr(lib, 'pardox_write_sharded_parquet'):
        lib.pardox_write_sharded_parquet.argtypes = [c_void_p, c_char_p, c_longlong]
        lib.pardox_write_sharded_parquet.restype = c_longlong
