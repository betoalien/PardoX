def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    for fn, args, ret in [
        ('pardox_live_query_start',   [c_char_p, c_char_p, c_longlong], c_void_p),
        ('pardox_live_query_version', [c_void_p],                       c_longlong),
        ('pardox_live_query_take',    [c_void_p],                       c_void_p),
        ('pardox_live_query_free',    [c_void_p],                       None),
    ]:
        if hasattr(lib, fn):
            getattr(lib, fn).argtypes = args
            getattr(lib, fn).restype  = ret
