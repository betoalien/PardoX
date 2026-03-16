def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    if hasattr(lib, 'pardox_groupby_agg'):
        lib.pardox_groupby_agg.argtypes = [c_void_p, c_char_p, c_char_p]
        lib.pardox_groupby_agg.restype = c_void_p

    if hasattr(lib, 'pardox_groupby_agg_prdx'):
        lib.pardox_groupby_agg_prdx.argtypes = [c_char_p, c_char_p, c_char_p]
        lib.pardox_groupby_agg_prdx.restype = c_void_p

    if hasattr(lib, 'pardox_chunked_groupby'):
        lib.pardox_chunked_groupby.argtypes = [c_void_p, c_char_p, c_char_p, c_longlong]
        lib.pardox_chunked_groupby.restype = c_void_p
