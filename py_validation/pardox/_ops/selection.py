from ..wrapper import lib


class SelectionMixin:

    def __getitem__(self, key):
        """
        Column Selection: df["col"] -> Series
        Filtering: df[mask_series] -> DataFrame (Filtered)
        """
        # Case 1: Column Selection
        if isinstance(key, str):
            from ..series import Series
            return Series(self, key)

        # Case 2: Boolean Filtering (df[df['A'] > 5])
        if hasattr(key, '_df') and hasattr(key, 'dtype'):

            if 'Boolean' not in str(key.dtype):
                raise TypeError(f"Filter key must be a Boolean Series. Got: {key.dtype}")

            if not hasattr(lib, 'pardox_apply_filter'):
                raise NotImplementedError("Filter application API missing in Core.")

            # Access the mask pointer through the parent DataFrame of the Series.
            mask_ptr = key._df._ptr
            mask_col = key.name.encode('utf-8')

            res_ptr = lib.pardox_apply_filter(self._ptr, mask_ptr, mask_col)
            if not res_ptr:
                raise RuntimeError("Filter operation returned null pointer.")

            return self.__class__._from_ptr(res_ptr)

        raise NotImplementedError(f"Selection with type {type(key)} not supported yet.")

    @property
    def iloc(self):
        """
        Purely integer-location based indexing for selection by position.
        Usage: df.iloc[100:200]
        """
        return self._IlocIndexer(self)

    class _IlocIndexer:
        def __init__(self, df):
            self._df = df

        def __getitem__(self, key):
            if isinstance(key, slice):
                # Resolve slice indices
                start = key.start if key.start is not None else 0
                stop = key.stop if key.stop is not None else self._df.shape[0]

                if start < 0: start = 0  # Simple clamping
                if stop < start: stop = start

                length = stop - start

                # Call Rust Slicing API
                if hasattr(lib, 'pardox_slice_manager'):
                    new_ptr = lib.pardox_slice_manager(self._df._ptr, start, length)
                    if not new_ptr:
                        raise RuntimeError("Slice operation returned null pointer.")
                    return self._df.__class__._from_ptr(new_ptr)
                else:
                    raise NotImplementedError("Slicing API missing in Core.")
            else:
                raise TypeError("iloc only supports slices (e.g., [0:10]) for now.")
