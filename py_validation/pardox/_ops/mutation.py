import ctypes
import json
from ..wrapper import lib, c_char_p, c_double, c_int32


class MutationMixin:

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

        return self  # Enable method chaining

    def join(self, other, on=None, left_on=None, right_on=None, how="inner"):
        """
        Joins with another PardoX DataFrame.
        """
        if not hasattr(other, '_ptr'):
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

        return self.__class__._from_ptr(result_ptr)

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

            return  # Success!

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

        return self  # Enable method chaining

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

        return self  # Enable method chaining
