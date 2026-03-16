import ctypes
import json
from ..wrapper import lib, c_char_p


class MetadataMixin:

    @property
    def shape(self):
        """
        Returns a tuple representing the dimensionality of the DataFrame.
        Format: (rows, columns)
        """
        if hasattr(lib, 'pardox_get_row_count'):
            rows = lib.pardox_get_row_count(self._ptr)
            cols = len(self.columns)
            return (rows, cols)
        return (0, 0)

    @property
    def columns(self):
        """
        Returns the column labels of the DataFrame.
        """
        schema = self._get_schema_metadata()
        if schema:
            return [col['name'] for col in schema.get('columns', [])]
        return []

    @property
    def dtypes(self):
        """
        Returns the data types in the DataFrame.
        """
        schema = self._get_schema_metadata()
        if schema:
            return {col['name']: col['type'] for col in schema.get('columns', [])}
        return {}

    def _get_schema_metadata(self):
        """
        Internal helper to fetch schema JSON from Rust.
        """
        if not hasattr(lib, 'pardox_get_schema_json'):
            return {}

        json_ptr = lib.pardox_get_schema_json(self._ptr)
        if not json_ptr:
            return {}

        # Thread-local buffer — do NOT call pardox_free_string
        json_str = ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
        return json.loads(json_str)

    @property
    def _manager_ptr(self):
        """Internal access to the pointer."""
        return self._ptr
