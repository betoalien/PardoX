def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    try:
        if hasattr(lib, 'pardox_cast_column'):
            lib.pardox_cast_column.argtypes = [c_void_p, c_char_p, c_char_p]
            lib.pardox_cast_column.restype = c_int32
    except AttributeError:
        pass

    try:
        if hasattr(lib, 'pardox_add_column'):
            lib.pardox_add_column.argtypes = [c_void_p, c_void_p, c_char_p]
            lib.pardox_add_column.restype = c_longlong

        if hasattr(lib, 'pardox_fill_na'):
            lib.pardox_fill_na.argtypes = [c_void_p, c_char_p, c_double]
            lib.pardox_fill_na.restype = c_longlong

        if hasattr(lib, 'pardox_round'):
            lib.pardox_round.argtypes = [c_void_p, c_char_p, c_int32]
            lib.pardox_round.restype = c_longlong

    except Exception:
        pass

    try:
        if hasattr(lib, 'pardox_read_json_bytes'):
            lib.pardox_read_json_bytes.argtypes = [c_char_p, c_size_t]
            lib.pardox_read_json_bytes.restype = c_void_p
    except Exception:
        pass
