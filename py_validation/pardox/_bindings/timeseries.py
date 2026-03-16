def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    for fn, args, ret in [
        ('pardox_ffill',       [c_void_p, c_char_p], c_void_p),
        ('pardox_bfill',       [c_void_p, c_char_p], c_void_p),
        ('pardox_interpolate', [c_void_p, c_char_p], c_void_p),
    ]:
        if hasattr(lib, fn):
            getattr(lib, fn).argtypes = args
            getattr(lib, fn).restype  = ret
