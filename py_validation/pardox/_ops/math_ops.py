import ctypes
from ..wrapper import lib


class MathMixin:

    def add(self, col_a: str, col_b: str):
        """
        Columnar addition (col_a + col_b) using native Rust SIMD-optimized iterators.
        Returns a new DataFrame with the result column "result_math_add".
        """
        if not hasattr(lib, 'pardox_math_add'):
            raise NotImplementedError("pardox_math_add not found in Core.")
        ptr = lib.pardox_math_add(self._ptr, col_a.encode('utf-8'), col_b.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_math_add returned null.")
        return self.__class__._from_ptr(ptr)

    def sub(self, col_a: str, col_b: str):
        """
        Columnar subtraction (col_a - col_b) using native Rust iterators.
        Returns a new DataFrame with the result column "result_math_sub".
        """
        if not hasattr(lib, 'pardox_math_sub'):
            raise NotImplementedError("pardox_math_sub not found in Core.")
        ptr = lib.pardox_math_sub(self._ptr, col_a.encode('utf-8'), col_b.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_math_sub returned null.")
        return self.__class__._from_ptr(ptr)

    def mul(self, col_a: str, col_b: str):
        """
        Columnar multiplication (col_a * col_b) using native Rust iterators.
        Returns a new DataFrame with the result column "result_mul".
        """
        if not hasattr(lib, 'pardox_series_mul'):
            raise NotImplementedError("pardox_series_mul not found in Core.")
        ptr = lib.pardox_series_mul(self._ptr, col_a.encode('utf-8'), self._ptr, col_b.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_series_mul returned null.")
        return self.__class__._from_ptr(ptr)

    def std(self, col: str) -> float:
        """
        Native standard deviation of a column (Bessel-corrected sample std dev).
        Returns float.
        """
        if not hasattr(lib, 'pardox_math_stddev'):
            raise NotImplementedError("pardox_math_stddev not found in Core.")
        return lib.pardox_math_stddev(self._ptr, col.encode('utf-8'))

    def min_max_scale(self, col: str):
        """
        Min-Max Scaler: normalizes column values to [0, 1] natively in Rust.
        Returns a new DataFrame with the normalized column "result_minmax".
        """
        if not hasattr(lib, 'pardox_math_minmax'):
            raise NotImplementedError("pardox_math_minmax not found in Core.")
        ptr = lib.pardox_math_minmax(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_math_minmax returned null.")
        return self.__class__._from_ptr(ptr)

    def sort_values(self, by: str, ascending: bool = True, gpu: bool = False):
        """
        Sort the DataFrame by the named column.

        Args:
            by (str): Column name to sort by.
            ascending (bool): Sort order (default True).
            gpu (bool): If True, use the GPU Bitonic sort pipeline (falls back to CPU).

        Returns:
            DataFrame: A new DataFrame with rows sorted.
        """
        if gpu:
            if not hasattr(lib, 'pardox_gpu_sort'):
                raise NotImplementedError("pardox_gpu_sort not found in Core.")
            ptr = lib.pardox_gpu_sort(self._ptr, by.encode('utf-8'))
            if not ptr:
                raise RuntimeError("pardox_gpu_sort returned null.")
        else:
            if not hasattr(lib, 'pardox_sort_values'):
                raise NotImplementedError("pardox_sort_values not found in Core.")
            descending = 0 if ascending else 1
            ptr = lib.pardox_sort_values(self._ptr, by.encode('utf-8'), ctypes.c_int(descending))
            if not ptr:
                raise RuntimeError("pardox_sort_values returned null.")
        return self.__class__._from_ptr(ptr)
