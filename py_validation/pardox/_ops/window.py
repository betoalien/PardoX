import ctypes
from ..wrapper import lib


class WindowMixin:

    def window_rolling(self, col: str, size: int = 3):
        """
        Rolling mean (moving average) over a numeric column.

        Args:
            col (str): Column name.
            size (int): Window size.

        Returns:
            DataFrame: New DataFrame with rolling mean column.
        """
        if not hasattr(lib, 'pardox_rolling_mean'):
            raise NotImplementedError("pardox_rolling_mean not found in Core.")
        ptr = lib.pardox_rolling_mean(self._ptr, col.encode('utf-8'), ctypes.c_longlong(size))
        if not ptr:
            raise RuntimeError("window_rolling returned null.")
        return self.__class__._from_ptr(ptr)

    def window_lag(self, col: str, n: int = 1):
        """
        Shift a column down by n positions (lag).

        Args:
            col (str): Column name.
            n (int): Number of positions to lag.

        Returns:
            DataFrame: New DataFrame with lagged column.
        """
        if not hasattr(lib, 'pardox_lag'):
            raise NotImplementedError("pardox_lag not found in Core.")
        ptr = lib.pardox_lag(self._ptr, col.encode('utf-8'), ctypes.c_longlong(n))
        if not ptr:
            raise RuntimeError("window_lag returned null.")
        return self.__class__._from_ptr(ptr)

    def window_lead(self, col: str, n: int = 1):
        """
        Shift a column up by n positions (lead).

        Args:
            col (str): Column name.
            n (int): Number of positions to lead.

        Returns:
            DataFrame: New DataFrame with lead column.
        """
        if not hasattr(lib, 'pardox_lead'):
            raise NotImplementedError("pardox_lead not found in Core.")
        ptr = lib.pardox_lead(self._ptr, col.encode('utf-8'), ctypes.c_longlong(n))
        if not ptr:
            raise RuntimeError("window_lead returned null.")
        return self.__class__._from_ptr(ptr)

    def window_rank(self, col: str):
        """
        Rank rows by the values of a column (ascending, 1-based).

        Args:
            col (str): Column name.

        Returns:
            DataFrame: New DataFrame with rank column.
        """
        if not hasattr(lib, 'pardox_rank'):
            raise NotImplementedError("pardox_rank not found in Core.")
        ptr = lib.pardox_rank(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("window_rank returned null.")
        return self.__class__._from_ptr(ptr)

    def window_row_number(self):
        """
        Add a sequential row number column (1-based).

        Returns:
            DataFrame: New DataFrame with row number column.
        """
        if not hasattr(lib, 'pardox_row_number'):
            raise NotImplementedError("pardox_row_number not found in Core.")
        ptr = lib.pardox_row_number(self._ptr)
        if not ptr:
            raise RuntimeError("window_row_number returned null.")
        return self.__class__._from_ptr(ptr)
