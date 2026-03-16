def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    if hasattr(lib, 'pardox_write_prdx_encrypted'):
        lib.pardox_write_prdx_encrypted.argtypes = [c_void_p, c_char_p, c_char_p, c_char_p]
        lib.pardox_write_prdx_encrypted.restype = c_longlong

    if hasattr(lib, 'pardox_read_prdx_encrypted'):
        lib.pardox_read_prdx_encrypted.argtypes = [c_char_p, c_char_p]
        lib.pardox_read_prdx_encrypted.restype = c_void_p
