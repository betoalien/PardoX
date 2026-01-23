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

        try:
            # 2. Decode the C-String
            return ctypes.cast(ascii_ptr, c_char_p).value.decode('utf-8')
        finally:
            # 3. Free memory
            if hasattr(lib, 'pardox_free_string'):
                lib.pardox_free_string(ascii_ptr)

    def _fetch_json_dump(self, limit):
        """Legacy helper for older DLLs."""
        if hasattr(lib, 'pardox_manager_to_json'):
            json_ptr = lib.pardox_manager_to_json(self._ptr, limit)
            if json_ptr:
                try:
                    return ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
                finally:
                    if hasattr(lib, 'pardox_free_string'):
                        lib.pardox_free_string(json_ptr)
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

        try:
            json_str = ctypes.cast(json_ptr, c_char_p).value.decode('utf-8')
            return json.loads(json_str)
        finally:
            if hasattr(lib, 'pardox_free_string'):
                lib.pardox_free_string(json_ptr)

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
    
