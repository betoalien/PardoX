import ctypes
from ..wrapper import lib


class SpillMixin:

    def spill_to_disk(self, path: str):
        """
        Serialize the DataFrame to disk for out-of-core processing.

        Args:
            path (str): File path to write the spilled data.

        Returns:
            int: Bytes written (positive on success, negative on error).
        """
        if not hasattr(lib, 'pardox_spill_to_disk'):
            raise NotImplementedError("pardox_spill_to_disk not found in Core.")
        result = lib.pardox_spill_to_disk(self._ptr, path.encode('utf-8'))
        if result < 0:
            raise RuntimeError(f"pardox_spill_to_disk failed with error code: {result}")
        return result

    @staticmethod
    def spill_from_disk(path: str):
        """
        Load a spilled DataFrame from disk.

        Args:
            path (str): File path to read the spilled data from.

        Returns:
            DataFrame: Loaded DataFrame.
        """
        if not hasattr(lib, 'pardox_spill_from_disk'):
            raise NotImplementedError("pardox_spill_from_disk not found in Core.")
        ptr = lib.pardox_spill_from_disk(path.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_spill_from_disk returned null.")
        # Import here to avoid circular at module load
        from ..frame import DataFrame
        return DataFrame._from_ptr(ptr)

    def chunked_groupby(self, group_col: str, agg_specs: dict, chunk_size: int = 100000):
        """
        Perform chunked GroupBy aggregation for large datasets.

        Args:
            group_col (str): Column to group by.
            agg_specs (dict): Aggregation specification, e.g., {"price": "sum"}.
            chunk_size (int): Number of rows to process per chunk.

        Returns:
            DataFrame: Aggregated result.
        """
        if not hasattr(lib, 'pardox_chunked_groupby'):
            raise NotImplementedError("pardox_chunked_groupby not found in Core.")
        import json
        agg_json = json.dumps(agg_specs).encode('utf-8')
        ptr = lib.pardox_chunked_groupby(
            self._ptr,
            group_col.encode('utf-8'),
            agg_json,
            chunk_size
        )
        if not ptr:
            raise RuntimeError("pardox_chunked_groupby returned null.")
        return self.__class__._from_ptr(ptr)

    def external_sort(self, by: str, ascending: bool = True, chunk_size: int = 100000):
        """
        External merge sort for large datasets that don't fit in memory.

        Args:
            by (str): Column name to sort by.
            ascending (bool): Sort direction (default True).
            chunk_size (int): Number of rows per sort chunk.

        Returns:
            DataFrame: Sorted DataFrame.
        """
        if not hasattr(lib, 'pardox_external_sort'):
            raise NotImplementedError("pardox_external_sort not found in Core.")
        descending = 0 if ascending else 1
        ptr = lib.pardox_external_sort(
            self._ptr,
            by.encode('utf-8'),
            ctypes.c_int32(descending),
            ctypes.c_longlong(chunk_size)
        )
        if not ptr:
            raise RuntimeError("pardox_external_sort returned null.")
        return self.__class__._from_ptr(ptr)

    @staticmethod
    def memory_usage() -> int:
        """
        Get the current memory usage of the PardoX engine.

        Returns:
            int: Memory usage in bytes.
        """
        if not hasattr(lib, 'pardox_memory_usage'):
            raise NotImplementedError("pardox_memory_usage not found in Core.")
        return int(lib.pardox_memory_usage())
