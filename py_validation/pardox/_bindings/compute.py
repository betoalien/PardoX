def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    try:
        if hasattr(lib, 'pardox_filter_compare'):
            lib.pardox_filter_compare.argtypes = [c_void_p, c_char_p, c_void_p, c_char_p, c_int32]
            lib.pardox_filter_compare.restype = c_void_p

        if hasattr(lib, 'pardox_filter_compare_scalar'):
            lib.pardox_filter_compare_scalar.argtypes = [
                c_void_p, c_char_p,
                c_double, c_longlong, c_int32,
                c_int32
            ]
            lib.pardox_filter_compare_scalar.restype = c_void_p
    except Exception:
        pass

    try:
        if hasattr(lib, 'pardox_apply_filter'):
            lib.pardox_apply_filter.argtypes = [c_void_p, c_void_p, c_char_p]
            lib.pardox_apply_filter.restype = c_void_p
    except Exception:
        pass

    try:
        for fn in ['pardox_agg_sum', 'pardox_agg_mean', 'pardox_agg_min',
                   'pardox_agg_max', 'pardox_agg_count', 'pardox_agg_std']:
            if hasattr(lib, fn):
                getattr(lib, fn).argtypes = [c_void_p, c_char_p]
                getattr(lib, fn).restype = c_double
    except Exception:
        pass

    for fn, args, ret in [
        ('pardox_series_add', [c_void_p, c_char_p, c_void_p, c_char_p], c_void_p),
        ('pardox_series_sub', [c_void_p, c_char_p, c_void_p, c_char_p], c_void_p),
        ('pardox_series_mul', [c_void_p, c_char_p, c_void_p, c_char_p], c_void_p),
        ('pardox_series_div', [c_void_p, c_char_p, c_void_p, c_char_p], c_void_p),
        ('pardox_series_mod', [c_void_p, c_char_p, c_void_p, c_char_p], c_void_p),
    ]:
        if hasattr(lib, fn):
            getattr(lib, fn).argtypes = args
            getattr(lib, fn).restype  = ret
