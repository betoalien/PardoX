from ..wrapper import lib


class SqlMixin:

    def sql(self, query: str):
        """
        Execute SQL over this in-memory DataFrame.
        The table is available as 'df' inside the query.

        Args:
            query (str): SQL query, e.g. "SELECT category, SUM(price) FROM df GROUP BY category".

        Returns:
            DataFrame: Query result.

        Example:
            result = df.sql("SELECT category, COUNT(*) as cnt FROM df GROUP BY category")
        """
        if not hasattr(lib, 'pardox_sql_query'):
            raise NotImplementedError("pardox_sql_query not found in Core.")
        ptr = lib.pardox_sql_query(self._ptr, query.encode('utf-8'))
        if not ptr:
            raise RuntimeError("sql() returned null. Check query syntax.")
        return self.__class__._from_ptr(ptr)
