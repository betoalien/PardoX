def bind(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, c_int16):
    if hasattr(lib, 'pardox_validate_contract'):
        lib.pardox_validate_contract.argtypes = [c_void_p, c_char_p]
        lib.pardox_validate_contract.restype = c_void_p

    if hasattr(lib, 'pardox_contract_violation_count'):
        lib.pardox_contract_violation_count.restype = c_longlong

    if hasattr(lib, 'pardox_get_quarantine_logs'):
        lib.pardox_get_quarantine_logs.restype = c_void_p

    if hasattr(lib, 'pardox_clear_quarantine'):
        lib.pardox_clear_quarantine.restype = None
