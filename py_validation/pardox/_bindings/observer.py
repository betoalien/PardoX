def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    if hasattr(lib, 'pardox_to_json_records'):
        lib.pardox_to_json_records.argtypes = [c_void_p]
        lib.pardox_to_json_records.restype = c_void_p

    if hasattr(lib, 'pardox_to_json_arrays'):
        lib.pardox_to_json_arrays.argtypes = [c_void_p]
        lib.pardox_to_json_arrays.restype = c_void_p

    if hasattr(lib, 'pardox_value_counts'):
        lib.pardox_value_counts.argtypes = [c_void_p, c_char_p]
        lib.pardox_value_counts.restype = c_void_p

    if hasattr(lib, 'pardox_unique'):
        lib.pardox_unique.argtypes = [c_void_p, c_char_p]
        lib.pardox_unique.restype = c_void_p
