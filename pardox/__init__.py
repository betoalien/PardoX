# pardox/__init__.py

from .frame import DataFrame
from .io import read_csv, read_sql, from_arrow, read_prdx

# Y lo exponemos públicamente aquí
__all__ = ["DataFrame", "read_csv", "read_sql", "from_arrow", "read_prdx", "Series"]