from ..wrapper import lib


class TimeSeriesMixin:

    def ffill(self, col: str):
        """
        Forward fill null/NaN values in a column.

        Args:
            col (str): Column name to fill.

        Returns:
            DataFrame: New DataFrame with filled column.
        """
        if not hasattr(lib, 'pardox_ffill'):
            raise NotImplementedError("pardox_ffill not found in Core.")
        ptr = lib.pardox_ffill(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_ffill returned null.")
        return self.__class__._from_ptr(ptr)

    def bfill(self, col: str):
        """
        Backward fill null/NaN values in a column.

        Args:
            col (str): Column name to fill.

        Returns:
            DataFrame: New DataFrame with filled column.
        """
        if not hasattr(lib, 'pardox_bfill'):
            raise NotImplementedError("pardox_bfill not found in Core.")
        ptr = lib.pardox_bfill(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_bfill returned null.")
        return self.__class__._from_ptr(ptr)

    def interpolate(self, col: str):
        """
        Linear interpolation of null/NaN values in a column.

        Args:
            col (str): Column name to interpolate.

        Returns:
            DataFrame: New DataFrame with interpolated column.
        """
        if not hasattr(lib, 'pardox_interpolate'):
            raise NotImplementedError("pardox_interpolate not found in Core.")
        ptr = lib.pardox_interpolate(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_interpolate returned null.")
        return self.__class__._from_ptr(ptr)
