import ctypes
from ..wrapper import lib


class DateTimeMixin:

    def date_extract(self, col: str, part: str):
        """
        Extract a component from a date column (stored as days-since-epoch Int64).

        Args:
            col (str): Column name.
            part (str): 'year', 'month', 'day', 'weekday'.

        Returns:
            DataFrame: New DataFrame with extracted Int64 column.
        """
        if not hasattr(lib, 'pardox_date_extract'):
            raise NotImplementedError("pardox_date_extract not found in Core.")
        ptr = lib.pardox_date_extract(self._ptr, col.encode('utf-8'), part.encode('utf-8'))
        if not ptr:
            raise RuntimeError("date_extract returned null.")
        return self.__class__._from_ptr(ptr)

    def date_format(self, col: str, fmt: str):
        """
        Format a date column as a string using strftime format.

        Args:
            col (str): Column name.
            fmt (str): Format string, e.g. '%Y-%m-%d', '%B %Y'.

        Returns:
            DataFrame: New DataFrame with Utf8 formatted column.
        """
        if not hasattr(lib, 'pardox_date_format'):
            raise NotImplementedError("pardox_date_format not found in Core.")
        ptr = lib.pardox_date_format(self._ptr, col.encode('utf-8'), fmt.encode('utf-8'))
        if not ptr:
            raise RuntimeError("date_format returned null.")
        return self.__class__._from_ptr(ptr)

    def date_add(self, col: str, days: int):
        """
        Add a number of days to a date column.

        Args:
            col (str): Column name.
            days (int): Days to add (negative to subtract).

        Returns:
            DataFrame: New DataFrame with shifted date column.
        """
        if not hasattr(lib, 'pardox_date_add'):
            raise NotImplementedError("pardox_date_add not found in Core.")
        ptr = lib.pardox_date_add(self._ptr, col.encode('utf-8'), ctypes.c_longlong(days))
        if not ptr:
            raise RuntimeError("date_add returned null.")
        return self.__class__._from_ptr(ptr)

    def date_diff(self, col_a: str, col_b: str, unit: str = "days"):
        """
        Compute the difference between two date columns.

        Args:
            col_a (str): First date column.
            col_b (str): Second date column.
            unit (str): 'days', 'months', or 'years'.

        Returns:
            DataFrame: New DataFrame with Int64 difference column.
        """
        if not hasattr(lib, 'pardox_date_diff'):
            raise NotImplementedError("pardox_date_diff not found in Core.")
        ptr = lib.pardox_date_diff(
            self._ptr,
            col_a.encode('utf-8'),
            col_b.encode('utf-8'),
            unit.encode('utf-8')
        )
        if not ptr:
            raise RuntimeError("date_diff returned null.")
        return self.__class__._from_ptr(ptr)
