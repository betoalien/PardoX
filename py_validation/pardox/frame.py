import ctypes
import json
from .wrapper import lib, c_char_p, c_size_t, c_int32, c_double

class DataFrame:
    def __init__(self, data, schema=None):
        """
        Initializes a PardoX DataFrame.
        """
        self._ptr = None

        # ---------------------------------------------------------
        # CASE 1: IN-MEMORY DATA (List of Dicts)
        # ---------------------------------------------------------
        if isinstance(data, list):
            if not data:
                raise ValueError("Cannot create DataFrame from empty list.")
            
            # CRITICAL FIX: Convert to NDJSON (Newline Delimited)
            # Arrow Reader prefers:
            # {"col":1}
            # {"col":2}
            # Instead of [{"col":1}, {"col":2}]
            try:
                # Generamos un string largo con saltos de línea
                ndjson_str = "\n".join([json.dumps(record) for record in data])
                json_bytes = ndjson_str.encode('utf-8')
                json_len = len(json_bytes)
            except Exception as e:
                raise ValueError(f"Failed to serialize data to NDJSON: {e}")

            # 2. Check Core Availability
            if not hasattr(lib, 'pardox_read_json_bytes'):
                raise NotImplementedError("Core API 'pardox_read_json_bytes' missing. Re-compile Rust.")

            # 3. Call Rust -> Returns a NEW Pointer (Isolated Manager)
            new_ptr = lib.pardox_read_json_bytes(json_bytes, json_len)
            
            if not new_ptr:
                # Si Rust devuelve Null, lanzamos error aquí mismo.
                raise RuntimeError("PardoX Core failed to ingest data (returned null pointer). Check console logs.")
            
            self._ptr = new_ptr

        # ---------------------------------------------------------
        # CASE 2: EXISTING POINTER (Internal / Native IO)
        # ---------------------------------------------------------
        elif isinstance(data, (int, ctypes.c_void_p)) or str(type(data)).find("LP_") != -1:
            if not data:
                raise ValueError("Null pointer received.")
            self._ptr = data

        else:
            raise TypeError(f"Invalid input type: {type(data)}")
        
    # =========================================================================
    # VISUALIZATION MAGIC
    # =========================================================================
    
    def __repr__(self):
        """
        Esta es la función mágica que Jupyter llama para mostrar el objeto.
        En lugar de devolver el objeto raw, devolvemos la tabla ASCII.
        """
        # Por defecto mostramos 10 filas al imprimir el objeto
        return self._fetch_ascii_table(10) or "<Empty PardoX DataFrame>"

    def head(self, n=5):
        """
        Ahora devuelve un NUEVO DataFrame con las primeras n filas.
        Al devolver un objeto, Jupyter llamará a su __repr__ y se verá bonito.
        """
        return self.iloc[0:n]

    def tail(self, n=5):
        """
        Devuelve un NUEVO DataFrame con las últimas n filas.
        """
        if not hasattr(lib, 'pardox_tail_manager'):
            raise NotImplementedError("tail() API not available in Core.")
        
        new_ptr = lib.pardox_tail_manager(self._ptr, n)
        if not new_ptr:
            raise RuntimeError("Failed to fetch tail.")
        
        return DataFrame(new_ptr)

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
    # METADATA & INSPECTION
    # =========================================================================

    @property
    def shape(self):
        """
        Returns a tuple representing the dimensionality of the DataFrame.
        Format: (rows, columns)
        """
        if hasattr(lib, 'pardox_get_row_count'):
            rows = lib.pardox_get_row_count(self._ptr)
            cols = len(self.columns) 
            return (rows, cols)
        return (0, 0)

    @property
    def columns(self):
        """
        Returns the column labels of the DataFrame.
        """
        schema = self._get_schema_metadata()
        if schema:
            return [col['name'] for col in schema.get('columns', [])]
        return []

    @property
    def dtypes(self):
        """
        Returns the data types in the DataFrame.
        """
        schema = self._get_schema_metadata()
        if schema:
            return {col['name']: col['type'] for col in schema.get('columns', [])}
        return {}

    # =========================================================================
    # SELECTION & SLICING (Indexer)
    # =========================================================================

    def __getitem__(self, key):
        """
        Column Selection: df["col"] -> Series
        Filtering: df[mask_series] -> DataFrame (Filtered)
        """
        # Case 1: Column Selection
        if isinstance(key, str):
            from .series import Series
            return Series(self, key)
        
        # Case 2: Boolean Filtering (df[df['A'] > 5])
        if hasattr(key, '_df') and hasattr(key, 'dtype'):
            
            if 'Boolean' not in str(key.dtype):
                raise TypeError(f"Filter key must be a Boolean Series. Got: {key.dtype}")
            
            if not hasattr(lib, 'pardox_apply_filter'):
                raise NotImplementedError("Filter application API missing in Core.")
            
            # Ahora sí accedemos al puntero a través del dataframe padre de la serie
            mask_ptr = key._df._ptr 
            mask_col = key.name.encode('utf-8')
            
            res_ptr = lib.pardox_apply_filter(self._ptr, mask_ptr, mask_col)
            if not res_ptr:
                raise RuntimeError("Filter operation returned null pointer.")
                
            return DataFrame(res_ptr)

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
                
                if start < 0: start = 0 # Simple clamping
                if stop < start: stop = start
                
                length = stop - start
                
                # Call Rust Slicing API
                if hasattr(lib, 'pardox_slice_manager'):
                    new_ptr = lib.pardox_slice_manager(self._df._ptr, start, length)
                    if not new_ptr:
                        raise RuntimeError("Slice operation returned null pointer.")
                    return DataFrame(new_ptr)
                else:
                    raise NotImplementedError("Slicing API missing in Core.")
            else:
                raise TypeError("iloc only supports slices (e.g., [0:10]) for now.")

    # =========================================================================
    # MUTATION & TRANSFORMATION
    # =========================================================================

    def cast(self, col_name, target_type):
        """
        Casts a column to a new type in-place.
        """
        if not hasattr(lib, 'pardox_cast_column'):
            raise NotImplementedError("Cast API missing.")

        res = lib.pardox_cast_column(
            self._ptr, 
            col_name.encode('utf-8'), 
            target_type.encode('utf-8')
        )
        
        if res != 1:
            raise RuntimeError(f"Failed to cast column '{col_name}' to '{target_type}'. Check compatibility.")
        
        return self # Enable method chaining

    def join(self, other, on=None, left_on=None, right_on=None, how="inner"):
        """
        Joins with another PardoX DataFrame.
        """
        if not isinstance(other, DataFrame):
            raise TypeError("The object to join must be a pardox.DataFrame")

        l_col = on if on else left_on
        r_col = on if on else right_on

        if not l_col or not r_col:
            raise ValueError("You must specify 'on' or ('left_on' and 'right_on')")

        # Call Rust Hash Join
        result_ptr = lib.pardox_hash_join(
            self._ptr,
            other._ptr,
            l_col.encode('utf-8'),
            r_col.encode('utf-8')
        )

        if not result_ptr:
            raise RuntimeError("Join failed (Rust returned null pointer).")

        return DataFrame(result_ptr)

    # =========================================================================
    # PERSISTENCE (IO WRITERS)
    # =========================================================================

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

    # =========================================================================
    # OBSERVER v0.3.1 — NATIVE UNIVERSAL EXPORT & VECTORIZED INSPECTION
    # =========================================================================

    def to_dict(self):
        """
        Returns ALL rows as a list of dicts (records format).
        Equivalent to Pandas' df.to_dict('records').

        Returns:
            list: A list of dicts [{col: val, ...}, ...].
        """
        if not hasattr(lib, 'pardox_to_json_records'):
            raise NotImplementedError("API 'pardox_to_json_records' not found in Core.")
        json_ptr = lib.pardox_to_json_records(self._ptr)
        if not json_ptr:
            return []
        try:
            json_str = ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
            return json.loads(json_str)
        finally:
            if hasattr(lib, 'pardox_free_string'):
                lib.pardox_free_string(json_ptr)

    def tolist(self):
        """
        Returns ALL rows as a list of lists (arrays format).
        Equivalent to Pandas' df.values.tolist().

        Returns:
            list: A list of lists [[val, val, ...], ...].
        """
        if not hasattr(lib, 'pardox_to_json_arrays'):
            raise NotImplementedError("API 'pardox_to_json_arrays' not found in Core.")
        json_ptr = lib.pardox_to_json_arrays(self._ptr)
        if not json_ptr:
            return []
        try:
            json_str = ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
            return json.loads(json_str)
        finally:
            if hasattr(lib, 'pardox_free_string'):
                lib.pardox_free_string(json_ptr)

    def to_json(self):
        """
        Serializes ALL rows to a JSON string (records format, no row limit).
        Unlike the preview helpers, this exports the full DataFrame.

        Returns:
            str: A JSON array string "[{...}, ...]".
        """
        if not hasattr(lib, 'pardox_to_json_records'):
            raise NotImplementedError("API 'pardox_to_json_records' not found in Core.")
        json_ptr = lib.pardox_to_json_records(self._ptr)
        if not json_ptr:
            return '[]'
        try:
            return ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
        finally:
            if hasattr(lib, 'pardox_free_string'):
                lib.pardox_free_string(json_ptr)

    def value_counts(self, col: str):
        """
        Returns the frequency of each unique value in the given column.

        Args:
            col (str): Column name.

        Returns:
            dict: {"value": count, ...} sorted by count descending.
        """
        if not hasattr(lib, 'pardox_value_counts'):
            raise NotImplementedError("API 'pardox_value_counts' not found in Core.")
        json_ptr = lib.pardox_value_counts(self._ptr, col.encode('utf-8'))
        if not json_ptr:
            return {}
        try:
            json_str = ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
            return json.loads(json_str)
        finally:
            if hasattr(lib, 'pardox_free_string'):
                lib.pardox_free_string(json_ptr)

    def unique(self, col: str):
        """
        Returns the unique values of the given column in insertion order.

        Args:
            col (str): Column name.

        Returns:
            list: A list of unique values.
        """
        if not hasattr(lib, 'pardox_unique'):
            raise NotImplementedError("API 'pardox_unique' not found in Core.")
        json_ptr = lib.pardox_unique(self._ptr, col.encode('utf-8'))
        if not json_ptr:
            return []
        try:
            json_str = ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
            return json.loads(json_str)
        finally:
            if hasattr(lib, 'pardox_free_string'):
                lib.pardox_free_string(json_ptr)

    # =========================================================================
    # NATIVE MATH FOUNDATION (v0.3.1)
    # =========================================================================

    def add(self, col_a: str, col_b: str):
        """
        Columnar addition (col_a + col_b) using native Rust SIMD-optimized iterators.
        Returns a new DataFrame with the result column "result_math_add".
        """
        if not hasattr(lib, 'pardox_math_add'):
            raise NotImplementedError("pardox_math_add not found in Core.")
        ptr = lib.pardox_math_add(self._ptr, col_a.encode('utf-8'), col_b.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_math_add returned null.")
        return DataFrame(ptr)

    def sub(self, col_a: str, col_b: str):
        """
        Columnar subtraction (col_a - col_b) using native Rust iterators.
        Returns a new DataFrame with the result column "result_math_sub".
        """
        if not hasattr(lib, 'pardox_math_sub'):
            raise NotImplementedError("pardox_math_sub not found in Core.")
        ptr = lib.pardox_math_sub(self._ptr, col_a.encode('utf-8'), col_b.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_math_sub returned null.")
        return DataFrame(ptr)

    def mul(self, col_a: str, col_b: str):
        """
        Columnar multiplication (col_a * col_b) using native Rust iterators.
        Returns a new DataFrame with the result column "result_mul".
        """
        if not hasattr(lib, 'pardox_series_mul'):
            raise NotImplementedError("pardox_series_mul not found in Core.")
        ptr = lib.pardox_series_mul(self._ptr, col_a.encode('utf-8'), self._ptr, col_b.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_series_mul returned null.")
        return DataFrame(ptr)

    def std(self, col: str) -> float:
        """
        Native standard deviation of a column (Bessel-corrected sample std dev).
        Returns float.
        """
        if not hasattr(lib, 'pardox_math_stddev'):
            raise NotImplementedError("pardox_math_stddev not found in Core.")
        return lib.pardox_math_stddev(self._ptr, col.encode('utf-8'))

    def min_max_scale(self, col: str):
        """
        Min-Max Scaler: normalizes column values to [0, 1] natively in Rust.
        Returns a new DataFrame with the normalized column "result_minmax".
        """
        if not hasattr(lib, 'pardox_math_minmax'):
            raise NotImplementedError("pardox_math_minmax not found in Core.")
        ptr = lib.pardox_math_minmax(self._ptr, col.encode('utf-8'))
        if not ptr:
            raise RuntimeError("pardox_math_minmax returned null.")
        return DataFrame(ptr)

    def sort_values(self, by: str, ascending: bool = True, gpu: bool = False):
        """
        Sort the DataFrame by the named column.

        Args:
            by (str): Column name to sort by.
            ascending (bool): Sort order (default True).
            gpu (bool): If True, use the GPU Bitonic sort pipeline (falls back to CPU).

        Returns:
            DataFrame: A new DataFrame with rows sorted.
        """
        if gpu:
            if not hasattr(lib, 'pardox_gpu_sort'):
                raise NotImplementedError("pardox_gpu_sort not found in Core.")
            ptr = lib.pardox_gpu_sort(self._ptr, by.encode('utf-8'))
            if not ptr:
                raise RuntimeError("pardox_gpu_sort returned null.")
        else:
            if not hasattr(lib, 'pardox_sort_values'):
                raise NotImplementedError("pardox_sort_values not found in Core.")
            descending = 0 if ascending else 1
            ptr = lib.pardox_sort_values(self._ptr, by.encode('utf-8'), ctypes.c_int(descending))
            if not ptr:
                raise RuntimeError("pardox_sort_values returned null.")
        return DataFrame(ptr)

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

    def _get_schema_metadata(self):
        """
        Internal helper to fetch schema JSON from Rust.
        """
        if not hasattr(lib, 'pardox_get_schema_json'):
            return {}

        json_ptr = lib.pardox_get_schema_json(self._ptr)
        if not json_ptr:
            return {}

        # Thread-local buffer — do NOT call pardox_free_string
        json_str = ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
        return json.loads(json_str)

    @property
    def _manager_ptr(self):
        """Internal access to the pointer."""
        return self._ptr
    
# =========================================================================
# MUTATION & FEATURE ENGINEERING (New in v0.1.5)
# =========================================================================

    def __setitem__(self, key, value):
        """
        Enables column assignment: df['new_col'] = df['a'] * df['b']
        """
        # 1. Check if value is a PardoX Series (Result of arithmetic)
        # CORRECCIÓN: Cambiamos '_col_name' por 'name' para coincidir con series.py
        if hasattr(value, '_df') and hasattr(value, 'name'):
            # It's a Series! We need to fuse it into this DataFrame.
            
            # Use the Series' parent DataFrame (which is a 1-column temporary DF)
            source_mgr_ptr = value._df._ptr
            col_name = key.encode('utf-8')

            if not hasattr(lib, 'pardox_add_column'):
                raise NotImplementedError("pardox_add_column API missing in Core.")

            # Call Rust Core to Move the column
            res = lib.pardox_add_column(self._ptr, source_mgr_ptr, col_name)

            if res != 1:
                error_map = {
                    -1: "Invalid Pointers",
                    -2: "Invalid Column Name String",
                    -3: "Engine Logic Error (Row mismatch or Duplicate Name)"
                }
                msg = error_map.get(res, f"Unknown Error: {res}")
                raise RuntimeError(f"Failed to assign column '{key}': {msg}")
            
            return # Success!

        # 2. Future support for scalar assignment (df['new'] = 0)
        # elif isinstance(value, (int, float, str)):
        #     self._assign_scalar(key, value)
        
        else:
            # Tip de Debugging: Imprimimos los atributos disponibles para ver qué pasó
            available_attrs = dir(value)
            raise TypeError(f"Assignment only supported for PardoX Series. Got: {type(value)}. Attributes detected: {available_attrs}")

    def fillna(self, value):
        """
        Fills Null/NaN values in the ENTIRE DataFrame with the specified scalar.
        This modifies the DataFrame in-place.
        """
        if not isinstance(value, (int, float)):
             raise TypeError("fillna currently only supports numeric scalars.")
             
        if not hasattr(lib, 'pardox_fill_na'):
            raise NotImplementedError("pardox_fill_na API missing in Core.")

        # Iterate over all numeric columns and apply fillna kernel
        # This is fast because the heavy lifting is done in Rust per column.
        current_schema = self.dtypes
        c_val = c_double(float(value))

        for col_name, dtype in current_schema.items():
            # Only apply to numeric types (Float/Int)
            if dtype in ["Float64", "Int64"]:
                res = lib.pardox_fill_na(
                    self._ptr, 
                    col_name.encode('utf-8'), 
                    c_val
                )
                if res != 1:
                    print(f"Warning: fillna failed for column '{col_name}'")
        
        return self # Enable method chaining

    def round(self, decimals=0):
        """
        Rounds all numeric columns to the specified number of decimals.
        This modifies the DataFrame in-place.
        """
        if not isinstance(decimals, int):
            raise TypeError("decimals must be an integer.")

        if not hasattr(lib, 'pardox_round'):
             raise NotImplementedError("pardox_round API missing in Core.")

        # Iterate over all columns. Rust kernel will safely ignore non-floats.
        current_columns = self.columns
        c_decimals = c_int32(decimals)

        for col_name in current_columns:
            # We call Rust blindly; the Kernel checks types internally for safety
            lib.pardox_round(
                self._ptr,
                col_name.encode('utf-8'),
                c_decimals
            )

        return self # Enable method chaining

    
