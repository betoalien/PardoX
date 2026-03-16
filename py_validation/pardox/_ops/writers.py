import json
from ..wrapper import lib


class WritersMixin:

    def to_csv(self, path_or_buf):
        """
        Exports the DataFrame to a CSV file.

        Args:
            path_or_buf (str): The file path where the CSV will be written.

        Returns:
            bool: True if successful.
        """
        if not isinstance(path_or_buf, str):
            raise TypeError("PardoX currently only supports writing to file paths (str).")

        if not hasattr(lib, 'pardox_to_csv'):
            raise NotImplementedError("API 'pardox_to_csv' not found in Core DLL. Re-compile Rust.")

        # Call Rust Core
        # Rust handles headers, buffering, and parallel iteration.
        res = lib.pardox_to_csv(self._ptr, path_or_buf.encode('utf-8'))

        if res != 1:
            error_map = {
                -1: "Invalid Manager Pointer",
                -2: "Invalid Path String",
                -3: "Failed to initialize CSV Writer (Check permissions/path)",
                -4: "Failed to write Header",
                -5: "Failed to write Data Block",
                -6: "Failed to flush buffer to disk"
            }
            msg = error_map.get(res, f"Unknown Error Code: {res}")
            raise RuntimeError(f"CSV Export Failed: {msg}")

        return True

    def to_sql(self, connection_string: str, table_name: str, mode: str = "append", conflict_cols: list = None):
        """
        Writes the DataFrame to a PostgreSQL table.

        Args:
            connection_string (str): PostgreSQL connection string.
            table_name (str): Target table name.
            mode (str): "append" (default) or "upsert".
            conflict_cols (list): Column names for ON CONFLICT clause (upsert only).

        Returns:
            int: Number of rows written.
        """
        # 1. Validate inputs
        if not isinstance(connection_string, str) or not connection_string:
            raise ValueError("connection_string must be a non-empty string.")
        if not isinstance(table_name, str) or not table_name:
            raise ValueError("table_name must be a non-empty string.")
        if mode not in ("append", "upsert"):
            raise ValueError("mode must be 'append' or 'upsert'.")

        if not hasattr(lib, 'pardox_write_sql'):
            raise NotImplementedError("API 'pardox_write_sql' not found in Core. Re-compile Rust.")

        # 2. Convert conflict_cols to a JSON string
        import json as _json
        cols_json = _json.dumps(conflict_cols if conflict_cols else [])

        # 3. Call Rust FFI
        rows_written = lib.pardox_write_sql(
            self._ptr,
            connection_string.encode('utf-8'),
            table_name.encode('utf-8'),
            mode.encode('utf-8'),
            cols_json.encode('utf-8'),
        )

        # 4. Handle errors
        if rows_written < 0:
            error_map = {
                -1: "Invalid Manager Pointer",
                -2: "Invalid connection string",
                -3: "Invalid table name",
                -4: "Invalid mode string",
                -5: "Invalid conflict_cols JSON",
                -100: "Write operation failed (check connection and table schema)",
            }
            msg = error_map.get(int(rows_written), f"Unknown Error Code: {rows_written}")
            raise RuntimeError(f"to_sql failed: {msg}")

        return int(rows_written)

    def to_mysql(self, connection_string: str, table_name: str, mode: str = "append", conflict_cols: list = None):
        """
        Writes the DataFrame to a MySQL table via the Rust native driver.

        Args:
            connection_string (str): MySQL URL (e.g. "mysql://user:pass@host:3306/db").
            table_name (str): Target table name.
            mode (str): "append" or "upsert".
            conflict_cols (list): Columns for ON DUPLICATE KEY UPDATE (upsert only).

        Returns:
            int: Number of rows written.
        """
        if not hasattr(lib, 'pardox_write_mysql'):
            raise NotImplementedError("API 'pardox_write_mysql' not found in Core. Re-compile Rust.")
        import json as _json
        cols_json = _json.dumps(conflict_cols if conflict_cols else [])
        rows_written = lib.pardox_write_mysql(
            self._ptr,
            connection_string.encode('utf-8'),
            table_name.encode('utf-8'),
            mode.encode('utf-8'),
            cols_json.encode('utf-8'),
        )
        if rows_written < 0:
            raise RuntimeError(f"to_mysql failed with error code: {rows_written}")
        return int(rows_written)

    def to_sqlserver(self, connection_string: str, table_name: str, mode: str = "append", conflict_cols: list = None):
        """
        Writes the DataFrame to a SQL Server table via the Rust native driver (tiberius).

        Args:
            connection_string (str): ADO.NET connection string.
                e.g. "Server=localhost,1433;Database=mydb;User Id=sa;Password=pwd;TrustServerCertificate=true"
            table_name (str): Target table name.
            mode (str): "append" or "upsert".
            conflict_cols (list): Columns for MERGE ON clause (upsert only).

        Returns:
            int: Number of rows written.
        """
        if not hasattr(lib, 'pardox_write_sqlserver'):
            raise NotImplementedError("API 'pardox_write_sqlserver' not found in Core. Re-compile Rust.")
        import json as _json
        cols_json = _json.dumps(conflict_cols if conflict_cols else [])
        rows_written = lib.pardox_write_sqlserver(
            self._ptr,
            connection_string.encode('utf-8'),
            table_name.encode('utf-8'),
            mode.encode('utf-8'),
            cols_json.encode('utf-8'),
        )
        if rows_written < 0:
            raise RuntimeError(f"to_sqlserver failed with error code: {rows_written}")
        return int(rows_written)

    def to_mongodb(self, connection_string: str, db_dot_collection: str, mode: str = "append"):
        """
        Writes the DataFrame to a MongoDB collection via the Rust native driver.

        Args:
            connection_string (str): MongoDB URI (e.g. "mongodb://user:pass@host:27017").
            db_dot_collection (str): Target as "database.collection" (e.g. "mydb.orders").
            mode (str): "append" (insert) or "replace" (drop + insert).

        Returns:
            int: Number of documents inserted.
        """
        if not hasattr(lib, 'pardox_write_mongodb'):
            raise NotImplementedError("API 'pardox_write_mongodb' not found in Core. Re-compile Rust.")
        rows_written = lib.pardox_write_mongodb(
            self._ptr,
            connection_string.encode('utf-8'),
            db_dot_collection.encode('utf-8'),
            mode.encode('utf-8'),
        )
        if rows_written < 0:
            raise RuntimeError(f"to_mongodb failed with error code: {rows_written}")
        return int(rows_written)

    def to_prdx(self, path_or_buf):
        """
        Exports the DataFrame to the native PardoX binary format (.prdx).
        This format supports Zero-Copy loading in future sessions.

        Args:
            path_or_buf (str): The file path (e.g., 'data.prdx').
        """
        if not isinstance(path_or_buf, str):
            raise TypeError("Path must be a string.")

        if not hasattr(lib, 'pardox_to_prdx'):
            # Fallback warning if you haven't exposed api_to_prdx in Rust yet
            raise NotImplementedError("API 'pardox_to_prdx' not available. Check api_writers.rs")

        res = lib.pardox_to_prdx(self._ptr, path_or_buf.encode('utf-8'))

        if res != 1:
            raise RuntimeError(f"PRDX Export Failed with error code: {res}")

        return True

    def to_parquet(self, path: str) -> int:
        """
        Write the DataFrame to a Parquet file.

        Args:
            path (str): Destination file path (e.g., "output.parquet").

        Returns:
            int: Number of rows written.
        """
        if not hasattr(lib, 'pardox_to_parquet'):
            raise NotImplementedError("pardox_to_parquet not found in Core.")
        result = lib.pardox_to_parquet(self._ptr, path.encode('utf-8'))
        if result < 0:
            raise RuntimeError(f"to_parquet failed with code: {result}")
        return int(result)

    def write_sharded_parquet(self, directory: str, max_rows_per_shard: int = 1_000_000) -> int:
        """
        Write the DataFrame to multiple Parquet files (shards) in a directory.

        Args:
            directory (str): Output directory path.
            max_rows_per_shard (int): Maximum rows per Parquet file. Default: 1,000,000.

        Returns:
            int: Number of rows written.
        """
        import ctypes as _ct
        if not hasattr(lib, 'pardox_write_sharded_parquet'):
            raise NotImplementedError("pardox_write_sharded_parquet not found in Core.")
        result = lib.pardox_write_sharded_parquet(
            self._ptr,
            directory.encode('utf-8'),
            _ct.c_longlong(max_rows_per_shard)
        )
        if result < 0:
            raise RuntimeError(f"write_sharded_parquet failed with code: {result}")
        return int(result)
