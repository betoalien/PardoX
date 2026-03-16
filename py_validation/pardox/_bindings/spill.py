import ctypes


def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    if hasattr(lib, 'pardox_spill_to_disk'):
        lib.pardox_spill_to_disk.argtypes = [c_void_p, c_char_p]
        lib.pardox_spill_to_disk.restype = c_longlong

    if hasattr(lib, 'pardox_spill_from_disk'):
        lib.pardox_spill_from_disk.argtypes = [c_char_p]
        lib.pardox_spill_from_disk.restype = c_void_p

    if hasattr(lib, 'pardox_external_sort'):
        lib.pardox_external_sort.argtypes = [c_void_p, c_char_p, c_int32, c_longlong]
        lib.pardox_external_sort.restype = c_void_p
