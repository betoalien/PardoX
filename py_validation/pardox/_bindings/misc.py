import ctypes


def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    if hasattr(lib, 'pardox_reset'):
        lib.pardox_reset.argtypes = []
        lib.pardox_reset.restype = None

    if hasattr(lib, 'pardox_create_block'):
        lib.pardox_create_block.argtypes = [c_char_p, c_char_p]
        lib.pardox_create_block.restype = c_void_p

    if hasattr(lib, 'pardox_append_block'):
        lib.pardox_append_block.argtypes = [c_void_p]
        lib.pardox_append_block.restype = c_longlong

    if hasattr(lib, 'pardox_scan_csv'):
        lib.pardox_scan_csv.argtypes = [c_char_p, c_char_p, c_char_p]
        lib.pardox_scan_csv.restype = c_longlong

    if hasattr(lib, 'pardox_hyper_copy_v3'):
        lib.pardox_hyper_copy_v3.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p]
        lib.pardox_hyper_copy_v3.restype = c_longlong

    if hasattr(lib, 'pardox_ingest_buffer'):
        lib.pardox_ingest_buffer.argtypes = [ctypes.c_void_p, c_size_t, c_char_p, c_char_p]
        lib.pardox_ingest_buffer.restype = c_void_p

    if hasattr(lib, 'pardox_inspect_file_lazy'):
        lib.pardox_inspect_file_lazy.argtypes = [c_char_p, c_char_p]
        lib.pardox_inspect_file_lazy.restype = c_char_p

    if hasattr(lib, 'pardox_legacy_ping'):
        lib.pardox_legacy_ping.argtypes = [c_char_p]
        lib.pardox_legacy_ping.restype = c_longlong

    if hasattr(lib, 'pardox_import_mainframe_dat'):
        lib.pardox_import_mainframe_dat.argtypes = [c_char_p, c_char_p, c_char_p]
        lib.pardox_import_mainframe_dat.restype = c_longlong

    if hasattr(lib, 'pardox_version'):
        lib.pardox_version.restype = c_void_p

    if hasattr(lib, 'pardox_get_system_report'):
        lib.pardox_get_system_report.restype = c_void_p

    if hasattr(lib, 'pardox_memory_usage'):
        lib.pardox_memory_usage.restype = c_longlong
