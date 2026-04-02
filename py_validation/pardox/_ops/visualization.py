import ctypes
from ..wrapper import lib, c_char_p


class VisualizationMixin:

    def __repr__(self):
        """
        Magic method used by Jupyter to render the object.
        Returns the ASCII table instead of the raw object representation.
        """
        # Show 10 rows by default when printing the object.
        return self._fetch_ascii_table(10) or "<Empty PardoX DataFrame>"

    def head(self, n=5):
        """
        Returns a NEW DataFrame with the first n rows.
        By returning an object, Jupyter calls __repr__ for rich display.
        """
        return self.iloc[0:n]

    def tail(self, n=5):
        """
        Returns a NEW DataFrame with the last n rows.
        """
        if not hasattr(lib, 'pardox_tail_manager'):
            raise NotImplementedError("tail() API not available in Core.")

        new_ptr = lib.pardox_tail_manager(self._ptr, n)
        if not new_ptr:
            raise RuntimeError("Failed to fetch tail.")

        return self.__class__._from_ptr(new_ptr)

    def show(self, n=10):
        """
        Prints the first n rows to the console explicitly.
        """
        ascii_table = self._fetch_ascii_table(n)
        if ascii_table:
            print(ascii_table)
        else:
            print(f"<PardoX DataFrame at {hex(self._ptr or 0)}> (Empty or Error)")

    # =========================================================================
    # INTERNAL HELPERS
    # =========================================================================

    def _fetch_ascii_table(self, limit):
        """
        Internal helper to fetch the ASCII table string from Rust.
        """
        if not hasattr(lib, 'pardox_manager_to_ascii'):
            return self._fetch_json_dump(limit)

        # 1. Call Rust to get the ASCII table string
        ascii_ptr = lib.pardox_manager_to_ascii(self._ptr, limit)

        if not ascii_ptr:
            return None

        # Decode the C-String — thread-local buffer, do NOT call pardox_free_string
        return ctypes.cast(ascii_ptr, c_char_p).value.decode('utf-8')

    def _fetch_json_dump(self, limit):
        """Legacy helper for older DLLs."""
        if hasattr(lib, 'pardox_manager_to_json'):
            json_ptr = lib.pardox_manager_to_json(self._ptr, limit)
            if json_ptr:
                # Thread-local buffer — do NOT call pardox_free_string
                return ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
        return "Inspection API missing."
