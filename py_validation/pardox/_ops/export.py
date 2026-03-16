import ctypes
import json
from ..wrapper import lib, c_char_p


class ExportMixin:

    def to_dict(self):
        """
        Returns ALL rows as a list of dicts (records format).
        Equivalent to Pandas' df.to_dict('records').

        Returns:
            list: A list of dicts [{col: val, ...}, ...].
        """
        if not hasattr(lib, 'pardox_to_json_records'):
            raise NotImplementedError("API 'pardox_to_json_records' not found in Core.")
        json_ptr = lib.pardox_to_json_records(self._ptr)
        if not json_ptr:
            return []
        try:
            json_str = ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
            return json.loads(json_str)
        finally:
            if hasattr(lib, 'pardox_free_string'):
                lib.pardox_free_string(json_ptr)

    def tolist(self):
        """
        Returns ALL rows as a list of lists (arrays format).
        Equivalent to Pandas' df.values.tolist().

        Returns:
            list: A list of lists [[val, val, ...], ...].
        """
        if not hasattr(lib, 'pardox_to_json_arrays'):
            raise NotImplementedError("API 'pardox_to_json_arrays' not found in Core.")
        json_ptr = lib.pardox_to_json_arrays(self._ptr)
        if not json_ptr:
            return []
        try:
            json_str = ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
            return json.loads(json_str)
        finally:
            if hasattr(lib, 'pardox_free_string'):
                lib.pardox_free_string(json_ptr)

    def to_json(self):
        """
        Serializes ALL rows to a JSON string (records format, no row limit).
        Unlike the preview helpers, this exports the full DataFrame.

        Returns:
            str: A JSON array string "[{...}, ...]".
        """
        if not hasattr(lib, 'pardox_to_json_records'):
            raise NotImplementedError("API 'pardox_to_json_records' not found in Core.")
        json_ptr = lib.pardox_to_json_records(self._ptr)
        if not json_ptr:
            return '[]'
        try:
            return ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
        finally:
            if hasattr(lib, 'pardox_free_string'):
                lib.pardox_free_string(json_ptr)

    def value_counts(self, col: str):
        """
        Returns the frequency of each unique value in the given column.

        Args:
            col (str): Column name.

        Returns:
            dict: {"value": count, ...} sorted by count descending.
        """
        if not hasattr(lib, 'pardox_value_counts'):
            raise NotImplementedError("API 'pardox_value_counts' not found in Core.")
        json_ptr = lib.pardox_value_counts(self._ptr, col.encode('utf-8'))
        if not json_ptr:
            return {}
        try:
            json_str = ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
            return json.loads(json_str)
        finally:
            if hasattr(lib, 'pardox_free_string'):
                lib.pardox_free_string(json_ptr)

    def unique(self, col: str):
        """
        Returns the unique values of the given column in insertion order.

        Args:
            col (str): Column name.

        Returns:
            list: A list of unique values.
        """
        if not hasattr(lib, 'pardox_unique'):
            raise NotImplementedError("API 'pardox_unique' not found in Core.")
        json_ptr = lib.pardox_unique(self._ptr, col.encode('utf-8'))
        if not json_ptr:
            return []
        try:
            json_str = ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
            return json.loads(json_str)
        finally:
            if hasattr(lib, 'pardox_free_string'):
                lib.pardox_free_string(json_ptr)
