import ctypes
from ..wrapper import lib


class ArrowSchema(ctypes.Structure):
    _fields_ = [
        ("format", ctypes.c_char_p),
        ("name", ctypes.c_char_p),
        ("metadata", ctypes.c_char_p),
        ("flags", ctypes.c_int64),
        ("n_children", ctypes.c_int64),
        ("children", ctypes.POINTER(ctypes.c_void_p)),
        ("dictionary", ctypes.c_void_p),
        ("release", ctypes.c_void_p),
        ("private_data", ctypes.c_void_p),
    ]


class ArrowArray(ctypes.Structure):
    _fields_ = [
        ("length", ctypes.c_int64),
        ("null_count", ctypes.c_int64),
        ("offset", ctypes.c_int64),
        ("n_buffers", ctypes.c_int64),
        ("n_children", ctypes.c_int64),
        ("buffers", ctypes.POINTER(ctypes.c_void_p)),
        ("children", ctypes.POINTER(ctypes.c_void_p)),
        ("dictionary", ctypes.c_void_p),
        ("release", ctypes.c_void_p),
        ("private_data", ctypes.c_void_p),
    ]


def from_arrow(data):
    """
    Ingests an Apache Arrow Table or RecordBatch into PardoX using Zero-Copy.

    Use this for sources not yet supported natively (e.g., Parquet, Snowflake via Arrow).
    """
    from ..frame import DataFrame
    try:
        import pyarrow as pa
    except ImportError as e:
        raise ImportError("from_arrow requires 'pyarrow' installed.") from e

    try:
        if isinstance(data, pa.Table):
            data = data.combine_chunks()
            if data.num_rows == 0:
                raise ValueError("Input Arrow Table is empty.")
            batch = data.to_batches()[0]
        elif isinstance(data, pa.RecordBatch):
            batch = data
        else:
            raise TypeError("Input must be a pyarrow.Table or pyarrow.RecordBatch.")

        if batch.num_rows == 0:
             raise ValueError("Input Arrow Batch is empty.")

        c_schema = ArrowSchema()
        c_array = ArrowArray()

        batch._export_to_c(
            ctypes.addressof(c_array),
            ctypes.addressof(c_schema)
        )

        mgr_ptr = lib.pardox_ingest_arrow_stream(
            ctypes.byref(c_array),
            ctypes.byref(c_schema)
        )

        if not mgr_ptr:
            raise RuntimeError("PardoX Core returned NULL pointer (Ingestion Failed).")

        return DataFrame(mgr_ptr)

    except Exception as e:
         raise RuntimeError(f"PardoX Arrow Ingestion Failed: {e}")
