from ..wrapper import lib


class StringsMixin:

    def str_upper(self, col: str):
        """Convert a string column to uppercase. Returns new DataFrame."""
        if not hasattr(lib, 'pardox_str_upper'):
            raise NotImplementedError("pardox_str_upper not found in Core.")
        ptr = lib.pardox_str_upper(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("str_upper returned null.")
        return self.__class__._from_ptr(ptr)

    def str_lower(self, col: str):
        """Convert a string column to lowercase. Returns new DataFrame."""
        if not hasattr(lib, 'pardox_str_lower'):
            raise NotImplementedError("pardox_str_lower not found in Core.")
        ptr = lib.pardox_str_lower(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("str_lower returned null.")
        return self.__class__._from_ptr(ptr)

    def str_len(self, col: str):
        """Compute the length of each string in a column. Returns new DataFrame with Int64 column."""
        if not hasattr(lib, 'pardox_str_len'):
            raise NotImplementedError("pardox_str_len not found in Core.")
        ptr = lib.pardox_str_len(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("str_len returned null.")
        return self.__class__._from_ptr(ptr)

    def str_trim(self, col: str):
        """Strip leading/trailing whitespace from a string column. Returns new DataFrame."""
        if not hasattr(lib, 'pardox_str_trim'):
            raise NotImplementedError("pardox_str_trim not found in Core.")
        ptr = lib.pardox_str_trim(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("str_trim returned null.")
        return self.__class__._from_ptr(ptr)

    def str_contains(self, col: str, pattern: str):
        """Filter/mask column to rows where string contains the pattern. Returns new DataFrame."""
        if not hasattr(lib, 'pardox_str_contains'):
            raise NotImplementedError("pardox_str_contains not found in Core.")
        ptr = lib.pardox_str_contains(self._ptr, col.encode('utf-8'), pattern.encode('utf-8'))
        if not ptr:
            raise RuntimeError("str_contains returned null.")
        return self.__class__._from_ptr(ptr)

    def str_replace(self, col: str, from_str: str, to_str: str):
        """Replace occurrences of from_str with to_str in a string column. Returns new DataFrame."""
        if not hasattr(lib, 'pardox_str_replace'):
            raise NotImplementedError("pardox_str_replace not found in Core.")
        ptr = lib.pardox_str_replace(
            self._ptr,
            col.encode('utf-8'),
            from_str.encode('utf-8'),
            to_str.encode('utf-8')
        )
        if not ptr:
            raise RuntimeError("str_replace returned null.")
        return self.__class__._from_ptr(ptr)
