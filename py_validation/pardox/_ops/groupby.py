import json
from ..wrapper import lib


class GroupByMixin:

    def groupby(self, by, agg_specs: dict):
        """
        Group by column(s) and aggregate.

        Args:
            by (str or list): Column(s) to group by.
            agg_specs (dict): e.g. {"price": "sum", "quantity": "mean"}.
                Supported: "sum", "mean", "min", "max", "count", "std".

        Returns:
            DataFrame: Aggregated result.

        Example:
            result = df.groupby("category", {"price": "sum", "quantity": "mean"})
        """
        if not hasattr(lib, 'pardox_groupby_agg'):
            raise NotImplementedError("pardox_groupby_agg not found in Core.")
        cols = by if isinstance(by, list) else [by]
        cols_json = json.dumps(cols).encode('utf-8')
        agg_json = json.dumps(agg_specs).encode('utf-8')
        ptr = lib.pardox_groupby_agg(self._ptr, cols_json, agg_json)
        if not ptr:
            raise RuntimeError("groupby returned null. Check column names and agg specs.")
        return self.__class__._from_ptr(ptr)
