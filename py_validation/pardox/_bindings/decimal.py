def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    for fn, args, ret in [
        ('pardox_decimal_from_float',  [c_void_p, c_char_p, c_int32],           c_void_p),
        ('pardox_decimal_sum',         [c_void_p, c_char_p],                    c_double),
        ('pardox_decimal_add',         [c_void_p, c_char_p, c_void_p, c_char_p], c_void_p),
        ('pardox_decimal_sub',         [c_void_p, c_char_p, c_void_p, c_char_p], c_void_p),
        ('pardox_decimal_mul_float',   [c_void_p, c_char_p, c_double],           c_void_p),
        ('pardox_decimal_round',       [c_void_p, c_char_p, c_int32],           c_void_p),
        ('pardox_decimal_to_float',    [c_void_p, c_char_p],                    c_void_p),
        ('pardox_decimal_get_scale',   [c_void_p, c_char_p],                    c_int32),
    ]:
        if hasattr(lib, fn):
            getattr(lib, fn).argtypes = args
            getattr(lib, fn).restype  = ret
