import ctypes


def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    if hasattr(lib, 'pardox_math_add'):
        lib.pardox_math_add.argtypes = [c_void_p, c_char_p, c_char_p]
        lib.pardox_math_add.restype = c_void_p

    if hasattr(lib, 'pardox_math_sub'):
        lib.pardox_math_sub.argtypes = [c_void_p, c_char_p, c_char_p]
        lib.pardox_math_sub.restype = c_void_p

    if hasattr(lib, 'pardox_math_stddev'):
        lib.pardox_math_stddev.argtypes = [c_void_p, c_char_p]
        lib.pardox_math_stddev.restype = c_double

    if hasattr(lib, 'pardox_math_minmax'):
        lib.pardox_math_minmax.argtypes = [c_void_p, c_char_p]
        lib.pardox_math_minmax.restype = c_void_p

    if hasattr(lib, 'pardox_sort_values'):
        lib.pardox_sort_values.argtypes = [c_void_p, c_char_p, ctypes.c_int]
        lib.pardox_sort_values.restype = c_void_p

    if hasattr(lib, 'pardox_gpu_sort'):
        lib.pardox_gpu_sort.argtypes = [c_void_p, c_char_p]
        lib.pardox_gpu_sort.restype = c_void_p
