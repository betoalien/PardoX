def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    if hasattr(lib, 'pardox_read_mysql'):
        lib.pardox_read_mysql.argtypes = [c_char_p, c_char_p]
        lib.pardox_read_mysql.restype = c_void_p

    if hasattr(lib, 'pardox_write_mysql'):
        lib.pardox_write_mysql.argtypes = [c_void_p, c_char_p, c_char_p, c_char_p, c_char_p]
        lib.pardox_write_mysql.restype = c_longlong

    if hasattr(lib, 'pardox_execute_mysql'):
        lib.pardox_execute_mysql.argtypes = [c_char_p, c_char_p]
        lib.pardox_execute_mysql.restype = c_longlong

    if hasattr(lib, 'pardox_read_sqlserver'):
        lib.pardox_read_sqlserver.argtypes = [c_char_p, c_char_p]
        lib.pardox_read_sqlserver.restype = c_void_p

    if hasattr(lib, 'pardox_write_sqlserver'):
        lib.pardox_write_sqlserver.argtypes = [c_void_p, c_char_p, c_char_p, c_char_p, c_char_p]
        lib.pardox_write_sqlserver.restype = c_longlong

    if hasattr(lib, 'pardox_execute_sqlserver'):
        lib.pardox_execute_sqlserver.argtypes = [c_char_p, c_char_p]
        lib.pardox_execute_sqlserver.restype = c_longlong

    if hasattr(lib, 'pardox_read_mongodb'):
        lib.pardox_read_mongodb.argtypes = [c_char_p, c_char_p]
        lib.pardox_read_mongodb.restype = c_void_p

    if hasattr(lib, 'pardox_write_mongodb'):
        lib.pardox_write_mongodb.argtypes = [c_void_p, c_char_p, c_char_p, c_char_p]
        lib.pardox_write_mongodb.restype = c_longlong

    if hasattr(lib, 'pardox_execute_mongodb'):
        lib.pardox_execute_mongodb.argtypes = [c_char_p, c_char_p, c_char_p]
        lib.pardox_execute_mongodb.restype = c_longlong
