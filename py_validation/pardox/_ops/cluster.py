from ..wrapper import lib


class ClusterMixin:

    def cluster_scatter(self, connection, name: str):
        """
        Scatter this DataFrame to a cluster connection.

        Args:
            connection: Cluster connection handle (from pdx.cluster_connect()).
            name (str): Dataset name to register on the cluster.

        Returns:
            DataFrame: Result pointer from scatter operation.
        """
        if not hasattr(lib, 'pardox_cluster_scatter'):
            raise NotImplementedError("pardox_cluster_scatter not found in Core.")
        ptr = lib.pardox_cluster_scatter(connection._ptr, name.encode('utf-8'), self._ptr)
        if not ptr:
            raise RuntimeError("cluster_scatter returned null.")
        return self.__class__._from_ptr(ptr)
