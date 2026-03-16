def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    # Cross-manager variant (4-arg)
    for fn, args, ret in [
        ('pardox_gpu_add', [c_void_p, c_char_p, c_void_p, c_char_p], c_void_p),
        ('pardox_gpu_mul', [c_void_p, c_char_p, c_void_p, c_char_p], c_void_p),
        ('pardox_gpu_sub', [c_void_p, c_char_p, c_void_p, c_char_p], c_void_p),
        ('pardox_gpu_div', [c_void_p, c_char_p, c_void_p, c_char_p], c_void_p),
        ('pardox_gpu_abs', [c_void_p, c_char_p],                      c_void_p),
        ('pardox_gpu_exp', [c_void_p, c_char_p],                      c_void_p),
        ('pardox_gpu_log', [c_void_p, c_char_p],                      c_void_p),
        ('pardox_gpu_sqrt',[c_void_p, c_char_p],                      c_void_p),
    ]:
        if hasattr(lib, fn):
            getattr(lib, fn).argtypes = args
            getattr(lib, fn).restype  = ret
