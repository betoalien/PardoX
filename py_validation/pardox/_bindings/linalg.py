def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    for fn, args, ret in [
        ('pardox_matmul',       [c_void_p, c_char_p, c_void_p, c_char_p], c_void_p),
        ('pardox_l2_normalize', [c_void_p, c_char_p],                     c_void_p),
        ('pardox_l1_normalize', [c_void_p, c_char_p],                     c_void_p),
        ('pardox_cosine_sim',   [c_void_p, c_char_p, c_void_p, c_char_p], c_double),
        ('pardox_pca',          [c_void_p, c_char_p, c_int32],             c_void_p),
    ]:
        if hasattr(lib, fn):
            getattr(lib, fn).argtypes = args
            getattr(lib, fn).restype  = ret
