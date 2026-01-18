import ctypes
from .wrapper import lib, c_double, c_longlong, c_int32

class Series:
    """
    Representa una columna individual de un PardoX DataFrame.
    Habilita operaciones vectorizadas (Aritmética Híbrida CPU/GPU) utilizando el motor Rust.
    """
    def __init__(self, df, col_name):
        self._df = df
        self.name = col_name

    def __repr__(self):
        # Muestra la tabla ASCII delegando al DataFrame padre.
        # Esto asegura que se vea bonito en Jupyter, reutilizando la lógica de frame.py
        return self._df.__repr__()

    @property
    def dtype(self):
        """Devuelve el tipo de dato de la serie consultando al DataFrame padre."""
        return self._df.dtypes.get(self.name, "Unknown")

    # =========================================================================
    # INSPECTION & VISUALIZATION
    # =========================================================================

    def head(self, n=5):
        """
        Devuelve una NUEVA Series con las primeras N filas.
        """
        # Obtenemos un DataFrame recortado (gracias a los cambios en frame.py)
        new_df = self._df.head(n)
        # Devolvemos una Series apuntando a la columna correspondiente en ese nuevo DF
        return new_df[self.name]

    def tail(self, n=5):
        """
        Devuelve una NUEVA Series con las últimas N filas.
        """
        new_df = self._df.tail(n)
        return new_df[self.name]

    def show(self, n=10):
        """
        Prints the series to the console using Rust's formatter.
        """
        self._df.show(n)

    # =========================================================================
    # ARITHMETIC OPERATIONS (Cross-Manager Support)
    # =========================================================================

    def _validate_ops(self, other):
        """
        Verifica que la operación sea segura.
        Permite Aritmética Cross-Manager si la cantidad de filas coincide.
        """
        if not isinstance(other, Series):
            raise TypeError(f"Operands must be PardoX Series objects. Got {type(other)}")
        
        if self._df.shape[0] != other._df.shape[0]:
            raise ValueError(f"Length mismatch: {self._df.shape[0]} vs {other._df.shape[0]}")
        
        return other

    def _wrap_result(self, res_ptr):
        """
        Envuelve el puntero resultante en una Series.
        Esto permite encadenar operaciones: (A + B) + C
        """
        if not res_ptr:
            raise RuntimeError("Operation failed (Rust returned null pointer).")
        
        from .frame import DataFrame
        new_df = DataFrame(res_ptr)
        
        if len(new_df.columns) == 0:
             raise RuntimeError("Compute engine returned empty result schema.")
             
        # Rust devuelve un nombre generado (ej. result_add), lo tomamos dinámicamente
        return new_df[new_df.columns[0]]

    # --- Operators ---

    def __add__(self, other):
        other = self._validate_ops(other)
        res_ptr = lib.pardox_series_add(
            self._df._ptr, 
            self.name.encode('utf-8'), 
            other._df._ptr, 
            other.name.encode('utf-8')
        )
        return self._wrap_result(res_ptr)

    def __sub__(self, other):
        other = self._validate_ops(other)
        res_ptr = lib.pardox_series_sub(
            self._df._ptr, 
            self.name.encode('utf-8'), 
            other._df._ptr,
            other.name.encode('utf-8')
        )
        return self._wrap_result(res_ptr)

    def __mul__(self, other):
        other = self._validate_ops(other)
        res_ptr = lib.pardox_series_mul(
            self._df._ptr, 
            self.name.encode('utf-8'), 
            other._df._ptr,
            other.name.encode('utf-8')
        )
        return self._wrap_result(res_ptr)

    def __truediv__(self, other):
        other = self._validate_ops(other)
        res_ptr = lib.pardox_series_div(
            self._df._ptr, 
            self.name.encode('utf-8'), 
            other._df._ptr,
            other.name.encode('utf-8')
        )
        return self._wrap_result(res_ptr)

    def __mod__(self, other):
        other = self._validate_ops(other)
        res_ptr = lib.pardox_series_mod(
            self._df._ptr, 
            self.name.encode('utf-8'), 
            other._df._ptr,
            other.name.encode('utf-8')
        )
        return self._wrap_result(res_ptr)

    # =========================================================================
    # COMPARISONS (FILTERS)
    # =========================================================================
    
    def _cmp_op(self, other, op_code):
        """
        Generic comparison handler.
        op_code mapping: 0=Eq, 1=Neq, 2=Gt, 3=Gte, 4=Lt, 5=Lte
        """
        # Case A: Column vs Column
        if isinstance(other, Series):
            if self._df.shape[0] != other._df.shape[0]:
                raise ValueError("Length mismatch in comparison")
            
            res_ptr = lib.pardox_filter_compare(
                self._df._ptr, self.name.encode('utf-8'),
                other._df._ptr, other.name.encode('utf-8'),
                op_code
            )
            return self._wrap_result(res_ptr)
        
        # Case B: Column vs Scalar
        elif isinstance(other, (int, float)):
            is_float = 1 if isinstance(other, float) else 0
            val_f64 = float(other)
            val_i64 = int(other)
            
            res_ptr = lib.pardox_filter_compare_scalar(
                self._df._ptr, self.name.encode('utf-8'),
                c_double(val_f64), c_longlong(val_i64), c_int32(is_float),
                op_code
            )
            return self._wrap_result(res_ptr)
        
        else:
            raise TypeError(f"Comparison not supported for type {type(other)}")

    def __eq__(self, other): return self._cmp_op(other, 0)
    def __ne__(self, other): return self._cmp_op(other, 1)
    def __gt__(self, other): return self._cmp_op(other, 2)
    def __ge__(self, other): return self._cmp_op(other, 3)
    def __lt__(self, other): return self._cmp_op(other, 4)
    def __le__(self, other): return self._cmp_op(other, 5)

    # =========================================================================
    # AGGREGATIONS
    # =========================================================================

    def _call_agg(self, func_name):
        if not hasattr(lib, func_name):
            raise NotImplementedError(f"Aggregation {func_name} not available in Core.")
        
        func = getattr(lib, func_name)
        val = func(self._df._ptr, self.name.encode('utf-8'))
        return val

    def sum(self): return self._call_agg('pardox_agg_sum')
    def mean(self): return self._call_agg('pardox_agg_mean')
    def min(self): return self._call_agg('pardox_agg_min')
    def max(self): return self._call_agg('pardox_agg_max')
    def count(self): return self._call_agg('pardox_agg_count')
    def std(self): return self._call_agg('pardox_agg_std')

# =========================================================================
# AGGREGATIONS (New in v0.1.5)
# Connects to Rust kernels defined in API 29
# =========================================================================

    def sum(self):
        """
        Returns the sum of values in the series.
        """
        if not hasattr(lib, 'pardox_agg_sum'):
            raise NotImplementedError("Aggregation 'sum' not found in Core.")
        
        return lib.pardox_agg_sum(self._df._ptr, self.name.encode('utf-8'))

    def mean(self):
        """
        Returns the arithmetic mean (average).
        """
        if not hasattr(lib, 'pardox_agg_mean'):
            raise NotImplementedError("Aggregation 'mean' not found in Core.")
        
        return lib.pardox_agg_mean(self._df._ptr, self.name.encode('utf-8'))

    def min(self):
        """
        Returns the minimum value.
        """
        if not hasattr(lib, 'pardox_agg_min'):
            raise NotImplementedError("Aggregation 'min' not found in Core.")
        
        return lib.pardox_agg_min(self._df._ptr, self.name.encode('utf-8'))

    def max(self):
        """
        Returns the maximum value.
        """
        if not hasattr(lib, 'pardox_agg_max'):
            raise NotImplementedError("Aggregation 'max' not found in Core.")
        
        return lib.pardox_agg_max(self._df._ptr, self.name.encode('utf-8'))

    def count(self):
        """
        Returns the count of non-null values.
        """
        if not hasattr(lib, 'pardox_agg_count'):
            raise NotImplementedError("Aggregation 'count' not found in Core.")
        
        # Count usually returns int, but our generic aggregator interface returns double currently.
        # We cast to int for Python consistency.
        return int(lib.pardox_agg_count(self._df._ptr, self.name.encode('utf-8')))

    def std(self):
        """
        Returns the standard deviation.
        """
        if not hasattr(lib, 'pardox_agg_std'):
            raise NotImplementedError("Aggregation 'std' not found in Core.")
        
        return lib.pardox_agg_std(self._df._ptr, self.name.encode('utf-8'))