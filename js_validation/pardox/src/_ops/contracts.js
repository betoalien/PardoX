'use strict';

const { getLib } = require('../ffi');

module.exports = {
    /**
     * Validate the DataFrame against a data contract.
     * @param {Object} rules  e.g. { amount: 'not_null' }
     * @returns {number} Number of violations (0 = all passed).
     */
    validateContract(rules) {
        const lib = getLib();
        return lib.pardox_validate_contract(this._ptr, JSON.stringify(rules));
    },
};
