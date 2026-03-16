def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    for fn, args, ret in [
        ('pardox_str_upper',    [c_void_p, c_char_p],                          c_void_p),
        ('pardox_str_lower',    [c_void_p, c_char_p],                          c_void_p),
        ('pardox_str_len',      [c_void_p, c_char_p],                          c_void_p),
        ('pardox_str_trim',     [c_void_p, c_char_p],                          c_void_p),
        ('pardox_str_replace',  [c_void_p, c_char_p, c_char_p, c_char_p],     c_void_p),
        ('pardox_str_contains', [c_void_p, c_char_p, c_char_p],               c_void_p),
    ]:
        if hasattr(lib, fn):
            getattr(lib, fn).argtypes = args
            getattr(lib, fn).restype  = ret
