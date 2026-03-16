def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    if hasattr(lib, 'pardox_pivot_table'):
        lib.pardox_pivot_table.argtypes = [c_void_p, c_char_p, c_char_p, c_char_p, c_char_p]
        lib.pardox_pivot_table.restype = c_void_p

    if hasattr(lib, 'pardox_melt'):
        lib.pardox_melt.argtypes = [c_void_p, c_char_p, c_char_p, c_char_p, c_char_p]
        lib.pardox_melt.restype = c_void_p

    if hasattr(lib, 'pardox_explode'):
        lib.pardox_explode.argtypes = [c_void_p, c_char_p]
        lib.pardox_explode.restype = c_void_p

    if hasattr(lib, 'pardox_unnest'):
        lib.pardox_unnest.argtypes = [c_void_p, c_char_p]
        lib.pardox_unnest.restype = c_void_p

    if hasattr(lib, 'pardox_json_extract'):
        lib.pardox_json_extract.argtypes = [c_void_p, c_char_p, c_char_p]
        lib.pardox_json_extract.restype = c_void_p
