from ..wrapper import lib, c_int32


class LinalgMixin:

    def linalg_l2_normalize(self, col: str):
        """
        L2-normalize a numeric column (divide each element by the L2 norm).

        Args:
            col (str): Column name.

        Returns:
            DataFrame: New DataFrame with normalized column.
        """
        if not hasattr(lib, 'pardox_l2_normalize'):
            raise NotImplementedError("pardox_l2_normalize not found in Core.")
        ptr = lib.pardox_l2_normalize(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("linalg_l2_normalize returned null.")
        return self.__class__._from_ptr(ptr)

    def linalg_l1_normalize(self, col: str):
        """
        L1-normalize a numeric column (divide each element by the L1 norm).

        Args:
            col (str): Column name.

        Returns:
            DataFrame: New DataFrame with normalized column.
        """
        if not hasattr(lib, 'pardox_l1_normalize'):
            raise NotImplementedError("pardox_l1_normalize not found in Core.")
        ptr = lib.pardox_l1_normalize(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("linalg_l1_normalize returned null.")
        return self.__class__._from_ptr(ptr)

    def linalg_cosine_sim(self, col_a: str, other, col_b: str) -> float:
        """
        Compute cosine similarity between two columns (possibly from different DataFrames).

        Args:
            col_a (str): Column in this DataFrame.
            other (DataFrame): Another DataFrame.
            col_b (str): Column in other DataFrame.

        Returns:
            float: Cosine similarity score [0, 1].
        """
        if not hasattr(lib, 'pardox_cosine_sim'):
            raise NotImplementedError("pardox_cosine_sim not found in Core.")
        return float(lib.pardox_cosine_sim(
            self._ptr, col_a.encode('utf-8'),
            other._ptr, col_b.encode('utf-8')
        ))

    def linalg_pca(self, col: str, n_components: int):
        """
        Principal Component Analysis on a numeric column.

        Args:
            col (str): Column name (must be Float64).
            n_components (int): Number of components to extract.

        Returns:
            DataFrame: New DataFrame with PCA result columns.
        """
        if not hasattr(lib, 'pardox_pca'):
            raise NotImplementedError("pardox_pca not found in Core.")
        ptr = lib.pardox_pca(self._ptr, col.encode('utf-8'), c_int32(n_components))
        if not ptr:
            raise RuntimeError("linalg_pca returned null.")
        return self.__class__._from_ptr(ptr)

    def linalg_matmul(self, col_a: str, other, col_b: str):
        """
        Matrix multiplication: col_a (this DF) × col_b (other DF).

        Args:
            col_a (str): Column in this DataFrame.
            other (DataFrame): Another DataFrame.
            col_b (str): Column in other DataFrame.

        Returns:
            DataFrame: New DataFrame with result column.
        """
        if not hasattr(lib, 'pardox_matmul'):
            raise NotImplementedError("pardox_matmul not found in Core.")
        ptr = lib.pardox_matmul(
            self._ptr, col_a.encode('utf-8'),
            other._ptr, col_b.encode('utf-8')
        )
        if not ptr:
            raise RuntimeError("linalg_matmul returned null.")
        return self.__class__._from_ptr(ptr)
