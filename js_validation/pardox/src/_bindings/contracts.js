'use strict';

module.exports = function bindContracts(_lib) {
    _lib._raw_pardox_validate_contract        = _lib.raw.func('int64_t pardox_validate_contract(void * mgr, const char * schema_json)');
    _lib._raw_pardox_contract_violation_count = _lib.raw.func('int64_t pardox_contract_violation_count()');
    _lib._raw_pardox_get_quarantine_logs      = _lib.raw.func('void * pardox_get_quarantine_logs()');
    _lib.pardox_clear_quarantine              = _lib.raw.func('void pardox_clear_quarantine()');
};
