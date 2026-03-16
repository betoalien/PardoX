def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    if hasattr(lib, 'pardox_read_rest'):
        lib.pardox_read_rest.argtypes = [c_char_p, c_char_p]
        lib.pardox_read_rest.restype = c_void_p
