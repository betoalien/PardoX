from ..wrapper import lib, c_int32, c_double


class DecimalMixin:

    def decimal_from_float(self, col: str, scale: int):
        """
        Convert a Float64 column to fixed-point Decimal.

        Args:
            col (str): Column name.
            scale (int): Decimal places (0–18).

        Returns:
            DataFrame: New DataFrame with Decimal column.
        """
        if not hasattr(lib, 'pardox_decimal_from_float'):
            raise NotImplementedError("pardox_decimal_from_float not found in Core.")
        ptr = lib.pardox_decimal_from_float(self._ptr, col.encode('utf-8'), c_int32(scale))
        if not ptr:
            raise RuntimeError("decimal_from_float returned null.")
        return self.__class__._from_ptr(ptr)

    def decimal_to_float(self, col: str):
        """Convert a Decimal column back to Float64. Returns new DataFrame."""
        if not hasattr(lib, 'pardox_decimal_to_float'):
            raise NotImplementedError("pardox_decimal_to_float not found in Core.")
        ptr = lib.pardox_decimal_to_float(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("decimal_to_float returned null.")
        return self.__class__._from_ptr(ptr)

    def decimal_round(self, col: str, scale: int):
        """Round a Decimal column to the given number of decimal places. Returns new DataFrame."""
        if not hasattr(lib, 'pardox_decimal_round'):
            raise NotImplementedError("pardox_decimal_round not found in Core.")
        ptr = lib.pardox_decimal_round(self._ptr, col.encode('utf-8'), c_int32(scale))
        if not ptr:
            raise RuntimeError("decimal_round returned null.")
        return self.__class__._from_ptr(ptr)

    def decimal_mul_float(self, col: str, factor: float):
        """Multiply a Decimal column by a float factor. Returns new DataFrame."""
        if not hasattr(lib, 'pardox_decimal_mul_float'):
            raise NotImplementedError("pardox_decimal_mul_float not found in Core.")
        ptr = lib.pardox_decimal_mul_float(self._ptr, col.encode('utf-8'), c_double(factor))
        if not ptr:
            raise RuntimeError("decimal_mul_float returned null.")
        return self.__class__._from_ptr(ptr)

    def decimal_sum(self, col: str) -> float:
        """Sum all values in a Decimal column. Returns float."""
        if not hasattr(lib, 'pardox_decimal_sum'):
            raise NotImplementedError("pardox_decimal_sum not found in Core.")
        return float(lib.pardox_decimal_sum(self._ptr, col.encode('utf-8')))
