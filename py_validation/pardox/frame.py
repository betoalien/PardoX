import ctypes
import json
from .wrapper import lib, c_char_p, c_size_t, c_int32, c_double, c_longlong
from ._ops import (
    VisualizationMixin, MetadataMixin, SelectionMixin, MutationMixin,
    WritersMixin, ExportMixin, MathMixin, GpuMixin, ReshapeMixin,
    TimeSeriesMixin, NestedMixin, SpillMixin, GroupByMixin, StringsMixin,
    DateTimeMixin, DecimalMixin, WindowMixin, SqlMixin, EncryptionMixin,
    ContractsMixin, TimeTravelMixin, ClusterMixin, LinalgMixin,
)


class DataFrame(
    VisualizationMixin, MetadataMixin, SelectionMixin, MutationMixin,
    WritersMixin, ExportMixin, MathMixin, GpuMixin, ReshapeMixin,
    TimeSeriesMixin, NestedMixin, SpillMixin, GroupByMixin, StringsMixin,
    DateTimeMixin, DecimalMixin, WindowMixin, SqlMixin, EncryptionMixin,
    ContractsMixin, TimeTravelMixin, ClusterMixin, LinalgMixin,
):
    @classmethod
    def _from_ptr(cls, ptr):
        obj = cls.__new__(cls)
        obj._ptr = ptr
        return obj

    def __init__(self, data=None, schema=None):
        """
        Initializes a PardoX DataFrame.
        """
        self._ptr = None

        if isinstance(data, dict):
            keys = list(data.keys())
            if not keys:
                data = []  # Diccionario vacío pasa a lista vacía
            else:
                # Asumimos que todas las columnas tienen la misma longitud
                length = len(data[keys[0]])
                data = [{k: data[k][i] for k in keys} for i in range(length)]

        # ---------------------------------------------------------
        # CASE 1: IN-MEMORY DATA (List of Dicts)
        # ---------------------------------------------------------
        if isinstance(data, list):
            if not data:
                raise ValueError("Cannot create DataFrame from empty list/dict.")

            # CRITICAL FIX: Convert to NDJSON (Newline Delimited)
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
                raise RuntimeError("PardoX Core failed to ingest data (returned null pointer). Check console logs.")

            self._ptr = new_ptr

        # ---------------------------------------------------------
        # CASE 2: EXISTING POINTER (Internal / Native IO)
        # ---------------------------------------------------------
        elif isinstance(data, (int, ctypes.c_void_p)) or str(type(data)).find("LP_") != -1:
            if not data:
                raise ValueError("Null pointer received.")
            self._ptr = data

        elif data is not None:
            raise TypeError(f"Invalid input type: {type(data)}")
