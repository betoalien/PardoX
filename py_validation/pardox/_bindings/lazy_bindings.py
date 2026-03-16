def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    for fn, args, ret in [
        ('pardox_lazy_scan_csv',    [c_char_p, c_char_p, c_char_p],    c_void_p),
        ('pardox_lazy_scan_prdx',   [c_char_p],                        c_void_p),
        ('pardox_lazy_filter',      [c_void_p, c_char_p, c_char_p, c_double], c_void_p),
        ('pardox_lazy_select',      [c_void_p, c_char_p],              c_void_p),
        ('pardox_lazy_limit',       [c_void_p, c_longlong],            c_void_p),
        ('pardox_lazy_collect',     [c_void_p],                        c_void_p),
        ('pardox_lazy_optimize',    [c_void_p],                        c_void_p),
        ('pardox_lazy_stats',       [c_void_p],                        c_void_p),
        ('pardox_lazy_describe',    [c_void_p],                        c_void_p),
        ('pardox_lazy_free',        [c_void_p],                        None),
        ('pardox_lazy_free_str',    [c_void_p],                        None),
    ]:
        if hasattr(lib, fn):
            getattr(lib, fn).argtypes = args
            getattr(lib, fn).restype  = ret
