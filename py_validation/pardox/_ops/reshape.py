import json
from ..wrapper import lib


class ReshapeMixin:

    def pivot_table(self, index: str, columns: str, values: str, agg_func: str = "sum"):
        """
        Create a pivot table from the DataFrame.

        Args:
            index (str): Column name to use as index (rows).
            columns (str): Column name to use for column headers.
            values (str): Column name with values to aggregate.
            agg_func (str): Aggregation function - "sum", "mean", "min", "max", "count".

        Returns:
            DataFrame: Pivoted DataFrame with index as rows and unique column values as columns.
        """
        if not hasattr(lib, 'pardox_pivot_table'):
            raise NotImplementedError("pardox_pivot_table not found in Core.")
        ptr = lib.pardox_pivot_table(
            self._ptr,
            index.encode('utf-8'),
            columns.encode('utf-8'),
            values.encode('utf-8'),
            agg_func.encode('utf-8')
        )
        if not ptr:
            raise RuntimeError(f"pivot_table returned null. Check column names and agg_func.")
        return self.__class__._from_ptr(ptr)

    def melt(self, id_vars: list, value_vars: list, var_name: str = "variable", value_name: str = "value"):
        """
        Unpivot a DataFrame from wide to long format.

        Args:
            id_vars (list): Column(s) to use as identifier variables.
            value_vars (list): Column(s) to unpivot (melt into rows).
            var_name (str): Name for the new 'variable' column.
            value_name (str): Name for the new 'value' column.

        Returns:
            DataFrame: Melted DataFrame in long format.
        """
        if not hasattr(lib, 'pardox_melt'):
            raise NotImplementedError("pardox_melt not found in Core.")
        id_vars_json = json.dumps(id_vars).encode('utf-8')
        value_vars_json = json.dumps(value_vars).encode('utf-8')
        ptr = lib.pardox_melt(
            self._ptr,
            id_vars_json,
            value_vars_json,
            var_name.encode('utf-8'),
            value_name.encode('utf-8')
        )
        if not ptr:
            raise RuntimeError("pardox_melt returned null. Check column names.")
        return self.__class__._from_ptr(ptr)
