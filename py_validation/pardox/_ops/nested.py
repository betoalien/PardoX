from ..wrapper import lib


class NestedMixin:

    def json_extract(self, col: str, key: str):
        """
        Extract a value from a JSON string column using dot notation.

        Args:
            col (str): Column name containing JSON strings.
            key (str): Key to extract (supports dot notation like "address.city").

        Returns:
            DataFrame: New DataFrame with extracted values as a single column.
        """
        if not hasattr(lib, 'pardox_json_extract'):
            raise NotImplementedError("pardox_json_extract not found in Core.")
        ptr = lib.pardox_json_extract(
            self._ptr,
            col.encode('utf-8'),
            key.encode('utf-8')
        )
        if not ptr:
            raise RuntimeError("pardox_json_extract returned null.")
        return self.__class__._from_ptr(ptr)

    def explode(self, col: str):
        """
        Explode a JSON array column into multiple rows.

        Args:
            col (str): Column name containing JSON arrays.

        Returns:
            DataFrame: New DataFrame with one row per array element.
        """
        if not hasattr(lib, 'pardox_explode'):
            raise NotImplementedError("pardox_explode not found in Core.")
        ptr = lib.pardox_explode(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_explode returned null.")
        return self.__class__._from_ptr(ptr)

    def unnest(self, col: str):
        """
        Unnest a JSON object column into multiple columns.

        Args:
            col (str): Column name containing JSON objects.

        Returns:
            DataFrame: New DataFrame with JSON keys as separate columns.
        """
        if not hasattr(lib, 'pardox_unnest'):
            raise NotImplementedError("pardox_unnest not found in Core.")
        ptr = lib.pardox_unnest(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_unnest returned null.")
        return self.__class__._from_ptr(ptr)
