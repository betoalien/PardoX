import ctypes


def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    for fn, args, ret in [
        ('pardox_cluster_worker_start',      [ctypes.c_uint16],                c_int32),
        ('pardox_cluster_worker_stop',       [],                               c_int32),
        ('pardox_cluster_worker_register',   [c_char_p, c_void_p],            c_int32),
        ('pardox_cluster_connect',           [c_char_p],                      c_void_p),
        ('pardox_cluster_scatter',           [c_void_p, c_char_p, c_void_p], c_void_p),
        ('pardox_cluster_sql',               [c_void_p, c_char_p, c_char_p], c_void_p),
        ('pardox_cluster_ping',              [c_void_p],                      c_int32),
        ('pardox_cluster_ping_all',          [c_void_p],                      c_int32),
        ('pardox_cluster_sql_resilient',     [c_void_p, c_char_p, c_char_p], c_void_p),
        ('pardox_cluster_scatter_resilient', [c_void_p, c_char_p, c_void_p], c_void_p),
        ('pardox_cluster_checkpoint',        [c_void_p, c_char_p],           c_int32),
        ('pardox_cluster_free',              [c_void_p],                      None),
    ]:
        if hasattr(lib, fn):
            getattr(lib, fn).argtypes = args
            getattr(lib, fn).restype  = ret
