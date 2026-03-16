import ctypes


def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    try:
        if hasattr(lib, 'pardox_manager_to_json'):
            lib.pardox_manager_to_json.argtypes = [c_void_p, c_size_t]
            lib.pardox_manager_to_json.restype = c_char_p

        if hasattr(lib, 'pardox_manager_to_ascii'):
            lib.pardox_manager_to_ascii.argtypes = [c_void_p, c_size_t]
            lib.pardox_manager_to_ascii.restype = c_char_p

        if hasattr(lib, 'pardox_get_row_count'):
            lib.pardox_get_row_count.argtypes = [c_void_p]
            lib.pardox_get_row_count.restype = c_int64

        if hasattr(lib, 'pardox_get_schema_json'):
            lib.pardox_get_schema_json.argtypes = [c_void_p]
            lib.pardox_get_schema_json.restype = c_char_p

    except AttributeError:
        pass

    try:
        if hasattr(lib, 'pardox_slice_manager'):
            lib.pardox_slice_manager.argtypes = [c_void_p, c_size_t, c_size_t]
            lib.pardox_slice_manager.restype = c_void_p

        if hasattr(lib, 'pardox_tail_manager'):
            lib.pardox_tail_manager.argtypes = [c_void_p, c_size_t]
            lib.pardox_tail_manager.restype = c_void_p

        if hasattr(lib, 'pardox_manager_to_json_range'):
            lib.pardox_manager_to_json_range.argtypes = [c_void_p, c_size_t, c_size_t]
            lib.pardox_manager_to_json_range.restype = c_char_p

    except AttributeError:
        pass

    for fn, args, ret in [
        ('pardox_get_column_names_json', [c_void_p], c_char_p),
        ('pardox_get_records_json',      [c_void_p], c_char_p),
        ('pardox_get_head_json',         [c_void_p, c_size_t], c_char_p),
    ]:
        if hasattr(lib, fn):
            getattr(lib, fn).argtypes = args
            getattr(lib, fn).restype  = ret

    if hasattr(lib, 'pardox_get_f64_buffer'):
        lib.pardox_get_f64_buffer.argtypes = [c_void_p, c_char_p, ctypes.POINTER(c_size_t)]
        lib.pardox_get_f64_buffer.restype = c_void_p

    if hasattr(lib, 'pardox_get_f64_buffer_all'):
        lib.pardox_get_f64_buffer_all.argtypes = [c_void_p, c_char_p, ctypes.POINTER(c_size_t)]
        lib.pardox_get_f64_buffer_all.restype = c_void_p

    if hasattr(lib, 'pardox_get_string_buffer'):
        lib.pardox_get_string_buffer.argtypes = [c_void_p, c_char_p, ctypes.POINTER(c_size_t)]
        lib.pardox_get_string_buffer.restype = c_void_p
