from ..wrapper import lib


class GpuMixin:

    def gpu_add(self, col_a: str, col_b: str):
        """
        GPU-accelerated column addition (col_a + col_b).
        Returns a new DataFrame with the result column "gpu_add".
        """
        if not hasattr(lib, 'pardox_gpu_add'):
            raise NotImplementedError("pardox_gpu_add not found in Core.")
        ptr = lib.pardox_gpu_add(self._ptr, col_a.encode('utf-8'), col_b.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_gpu_add returned null.")
        return self.__class__._from_ptr(ptr)

    def gpu_sub(self, col_a: str, col_b: str):
        """
        GPU-accelerated column subtraction (col_a - col_b).
        Returns a new DataFrame with the result column "gpu_sub".
        """
        if not hasattr(lib, 'pardox_gpu_sub'):
            raise NotImplementedError("pardox_gpu_sub not found in Core.")
        ptr = lib.pardox_gpu_sub(self._ptr, col_a.encode('utf-8'), col_b.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_gpu_sub returned null.")
        return self.__class__._from_ptr(ptr)

    def gpu_mul(self, col_a: str, col_b: str):
        """
        GPU-accelerated column multiplication (col_a * col_b).
        Returns a new DataFrame with the result column "gpu_mul".
        """
        if not hasattr(lib, 'pardox_gpu_mul'):
            raise NotImplementedError("pardox_gpu_mul not found in Core.")
        ptr = lib.pardox_gpu_mul(self._ptr, col_a.encode('utf-8'), col_b.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_gpu_mul returned null.")
        return self.__class__._from_ptr(ptr)

    def gpu_div(self, col_a: str, col_b: str):
        """
        GPU-accelerated column division (col_a / col_b).
        Returns a new DataFrame with the result column "gpu_div".
        """
        if not hasattr(lib, 'pardox_gpu_div'):
            raise NotImplementedError("pardox_gpu_div not found in Core.")
        ptr = lib.pardox_gpu_div(self._ptr, col_a.encode('utf-8'), col_b.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_gpu_div returned null.")
        return self.__class__._from_ptr(ptr)

    def gpu_sqrt(self, col: str):
        """
        GPU-accelerated square root of a column.
        Returns a new DataFrame with the result column "gpu_sqrt".
        """
        if not hasattr(lib, 'pardox_gpu_sqrt'):
            raise NotImplementedError("pardox_gpu_sqrt not found in Core.")
        ptr = lib.pardox_gpu_sqrt(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_gpu_sqrt returned null.")
        return self.__class__._from_ptr(ptr)

    def gpu_exp(self, col: str):
        """
        GPU-accelerated exponential (e^col).
        Returns a new DataFrame with the result column "gpu_exp".
        """
        if not hasattr(lib, 'pardox_gpu_exp'):
            raise NotImplementedError("pardox_gpu_exp not found in Core.")
        ptr = lib.pardox_gpu_exp(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_gpu_exp returned null.")
        return self.__class__._from_ptr(ptr)

    def gpu_log(self, col: str):
        """
        GPU-accelerated natural logarithm (ln(col)).
        Returns a new DataFrame with the result column "gpu_log".
        """
        if not hasattr(lib, 'pardox_gpu_log'):
            raise NotImplementedError("pardox_gpu_log not found in Core.")
        ptr = lib.pardox_gpu_log(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_gpu_log returned null.")
        return self.__class__._from_ptr(ptr)

    def gpu_abs(self, col: str):
        """
        GPU-accelerated absolute value of a column.
        Returns a new DataFrame with the result column "gpu_abs".
        """
        if not hasattr(lib, 'pardox_gpu_abs'):
            raise NotImplementedError("pardox_gpu_abs not found in Core.")
        ptr = lib.pardox_gpu_abs(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_gpu_abs returned null.")
        return self.__class__._from_ptr(ptr)
