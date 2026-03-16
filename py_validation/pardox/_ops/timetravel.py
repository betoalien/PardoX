import ctypes
from ..wrapper import lib


class TimeTravelMixin:

    def version_write(self, path: str, label: str, timestamp: int = 0):
        """
        Write a versioned snapshot of this DataFrame.

        Args:
            path (str): Base directory for version storage.
            label (str): Version label (e.g. 'v1', '2026-03-14').
            timestamp (int): Unix timestamp (0 = use current time).

        Returns:
            int: Row count written.
        """
        if not hasattr(lib, 'pardox_version_write'):
            raise NotImplementedError("pardox_version_write not found in Core.")
        result = lib.pardox_version_write(
            self._ptr,
            path.encode('utf-8'),
            label.encode('utf-8'),
            ctypes.c_longlong(timestamp)
        )
        if result < 0:
            raise RuntimeError(f"version_write failed with code: {result}")
        return int(result)
