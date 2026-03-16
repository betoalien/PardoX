import json
from ..wrapper import lib


class ClusterConnection:
    """Handle to a cluster of PardoX worker nodes."""

    def __init__(self, ptr):
        self._ptr = ptr

    def ping(self) -> int:
        if not hasattr(lib, 'pardox_cluster_ping'):
            raise NotImplementedError("pardox_cluster_ping not found in Core.")
        return int(lib.pardox_cluster_ping(self._ptr))

    def ping_all(self) -> int:
        if not hasattr(lib, 'pardox_cluster_ping_all'):
            raise NotImplementedError("pardox_cluster_ping_all not found in Core.")
        return int(lib.pardox_cluster_ping_all(self._ptr))

    def sql(self, name: str, query: str):
        from ..frame import DataFrame
        if not hasattr(lib, 'pardox_cluster_sql'):
            raise NotImplementedError("pardox_cluster_sql not found in Core.")
        ptr = lib.pardox_cluster_sql(self._ptr, name.encode('utf-8'), query.encode('utf-8'))
        if not ptr:
            raise RuntimeError(f"cluster.sql({name!r}) returned null.")
        return DataFrame(ptr)

    def checkpoint(self, path: str) -> int:
        if not hasattr(lib, 'pardox_cluster_checkpoint'):
            raise NotImplementedError("pardox_cluster_checkpoint not found in Core.")
        return int(lib.pardox_cluster_checkpoint(self._ptr, path.encode('utf-8')))

    def free(self):
        if self._ptr and hasattr(lib, 'pardox_cluster_free'):
            lib.pardox_cluster_free(self._ptr)
            self._ptr = None

    def __del__(self):
        self.free()

    def __repr__(self):
        return "<PardoX ClusterConnection>"


def cluster_connect(addrs: list):
    """
    Connect to a cluster of PardoX worker nodes.

    Example:
        conn = pdx.cluster_connect(["127.0.0.1:9001"])
        print(conn.ping())
    """
    if not hasattr(lib, 'pardox_cluster_connect'):
        raise NotImplementedError("pardox_cluster_connect not found in Core.")
    ptr = lib.pardox_cluster_connect(json.dumps(addrs).encode('utf-8'))
    if not ptr:
        raise RuntimeError(f"cluster_connect failed for: {addrs}")
    return ClusterConnection(ptr)
