import json
from ..wrapper import lib


class ContractsMixin:

    def validate_contract(self, rules: dict):
        """
        Validate the DataFrame against a contract.

        Args:
            rules (dict): Contract rules, e.g.:
                {"columns": [{"name": "price", "not_null": true, "min": 0}]}

        Returns:
            DataFrame: DataFrame containing violations (empty rows = valid).
        """
        if not hasattr(lib, 'pardox_validate_contract'):
            raise NotImplementedError("pardox_validate_contract not found in Core.")
        rules_json = json.dumps(rules).encode('utf-8')
        ptr = lib.pardox_validate_contract(self._ptr, rules_json)
        if not ptr:
            raise RuntimeError("validate_contract returned null.")
        return self.__class__._from_ptr(ptr)
