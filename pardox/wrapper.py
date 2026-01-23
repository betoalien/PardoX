import ctypes
import os
import sys
import platform

# 1. Detect OS and Select Library Path (Dynamic Architecture Support)
system_os = sys.platform
current_dir = os.path.dirname(os.path.abspath(__file__))

# Default to empty; filled by logic below
lib_name = ""
lib_folder = ""

if system_os == "win32":
    # Windows (Standard x64)
    lib_name = "pardox-cpu-Windows-x64.dll"
    lib_folder = os.path.join(current_dir, "libs", "Win")

elif system_os == "linux":
    # Linux (Standard x64)
    lib_name = "pardox-cpu-Linux-x64.so"
    lib_folder = os.path.join(current_dir, "libs", "Linux")

elif system_os == "darwin":
    # MacOS (Dual Architecture Support)
    lib_folder = os.path.join(current_dir, "libs", "Mac")
    
    # Check CPU Architecture (arm64 vs x86_64)
    machine_arch = platform.machine().lower()
    
    if "arm64" in machine_arch:
        # Apple Silicon (M1/M2/M3)
        lib_name = "pardox-cpu-MacOS-ARM64.dylib"
    elif "x86_64" in machine_arch:
        # Intel Mac
        lib_name = "pardox-cpu-MacOS-Intel.dylib"
    else:
        raise OSError(f"Unsupported MacOS Architecture: {machine_arch}. PardoX requires x86_64 or arm64.")

else:
    raise OSError(f"Unsupported Operating System: {system_os}")

# 2. Build absolute path
lib_path = os.path.join(lib_folder, lib_name)

# 3. Load the Library
if not os.path.exists(lib_path):
    raise ImportError(
        f"PardoX Core binary not found.\n"
        f"Expected at: {lib_path}\n"
        f"Please verify that the 'libs/' folder contains the correct binaries for your OS."
    )

try:
    lib = ctypes.CDLL(lib_path)
except OSError as e:
    raise ImportError(f"Failed to load PardoX Core at: {lib_path}.\nError details: {e}")

# 4. Define C Types
c_void_p = ctypes.c_void_p
c_char_p = ctypes.c_char_p
c_longlong = ctypes.c_longlong
c_double = ctypes.c_double  
c_int32 = ctypes.c_int32
c_size_t = ctypes.c_size_t
c_int64 = ctypes.c_int64

# =============================================================================
# API BINDINGS
# =============================================================================

# -- CORE: DATA LOADING --
# pardox_load_manager_csv(path, schema_json, config_json) -> *Manager
lib.pardox_load_manager_csv.argtypes = [c_char_p, c_char_p, c_char_p]
lib.pardox_load_manager_csv.restype = c_void_p

# -- CORE: NATIVE SQL --
# pardox_scan_sql(conn_str, query) -> *Manager
try:
    if hasattr(lib, 'pardox_scan_sql'):
        lib.pardox_scan_sql.argtypes = [c_char_p, c_char_p]
        lib.pardox_scan_sql.restype = c_void_p
except AttributeError:
    pass

# -- CORE: JOINS --
# pardox_hash_join(left, right, left_key, right_key) -> *Manager
lib.pardox_hash_join.argtypes = [c_void_p, c_void_p, c_char_p, c_char_p] 
lib.pardox_hash_join.restype = c_void_p

# -- CORE: MEMORY MANAGEMENT --
# pardox_free_manager(*Manager)
lib.pardox_free_manager.argtypes = [c_void_p]
lib.pardox_free_manager.restype = None

# pardox_free_string(*char)
try:
    lib.pardox_free_string.argtypes = [c_char_p]
    lib.pardox_free_string.restype = None
except AttributeError:
    print("Warning: pardox_free_string not found. Memory leaks possible.")

# -- EXTENSIONS: ARROW (Ingestion) --
try:
    lib.pardox_ingest_arrow_stream.argtypes = [c_void_p, c_void_p]
    lib.pardox_ingest_arrow_stream.restype = c_void_p
except AttributeError:
    pass

# -- EXTENSIONS: INSPECTION & METADATA (Phase 1) --
try:
    # 1. JSON Export
    lib.pardox_manager_to_json.argtypes = [c_void_p, c_size_t]
    lib.pardox_manager_to_json.restype = c_char_p 

    # 2. Native ASCII Table Export
    if hasattr(lib, 'pardox_manager_to_ascii'):
        lib.pardox_manager_to_ascii.argtypes = [c_void_p, c_size_t]
        lib.pardox_manager_to_ascii.restype = c_char_p

    # 3. Shape
    if hasattr(lib, 'pardox_get_row_count'):
        lib.pardox_get_row_count.argtypes = [c_void_p]
        lib.pardox_get_row_count.restype = c_int64

    # 4. Schema
    if hasattr(lib, 'pardox_get_schema_json'):
        lib.pardox_get_schema_json.argtypes = [c_void_p]
        lib.pardox_get_schema_json.restype = c_char_p

except AttributeError:
    print("Warning: Inspection API functions missing in DLL.")

# =============================================================================
# EXTENSIONS: SLICING & NAVIGATION (Phase 1.5)
# =============================================================================
try:
    # pardox_slice_manager(mgr, start, len) -> *Manager
    if hasattr(lib, 'pardox_slice_manager'):
        lib.pardox_slice_manager.argtypes = [c_void_p, c_size_t, c_size_t]
        lib.pardox_slice_manager.restype = c_void_p

    # pardox_tail_manager(mgr, n) -> *Manager
    if hasattr(lib, 'pardox_tail_manager'):
        lib.pardox_tail_manager.argtypes = [c_void_p, c_size_t]
        lib.pardox_tail_manager.restype = c_void_p

    # pardox_manager_to_json_range(mgr, start, limit) -> *char
    if hasattr(lib, 'pardox_manager_to_json_range'):
        lib.pardox_manager_to_json_range.argtypes = [c_void_p, c_size_t, c_size_t]
        lib.pardox_manager_to_json_range.restype = c_char_p

except AttributeError:
    print("Warning: Slicing API functions missing (Update Rust Core).")

# =============================================================================
# EXTENSIONS: CASTING & MUTATION (Phase 2 - Prep)
# =============================================================================
try:
    # pardox_cast_column(mgr, col_name, target_type) -> int (1=ok, -1=err)
    if hasattr(lib, 'pardox_cast_column'):
        lib.pardox_cast_column.argtypes = [c_void_p, c_char_p, c_char_p]
        lib.pardox_cast_column.restype = c_int32
except AttributeError:
    pass

# =============================================================================
# EXTENSIONS: HYBRID ARITHMETIC (Phase 2 - Calculator)
# =============================================================================
# All these functions now take (left_mgr, left_col, right_mgr, right_col) 
# and return a new *Manager containing the result column.
try:
    # ADD
    if hasattr(lib, 'pardox_series_add'):
        lib.pardox_series_add.argtypes = [c_void_p, c_char_p, c_void_p, c_char_p]
        lib.pardox_series_add.restype = c_void_p
    
    # SUB
    if hasattr(lib, 'pardox_series_sub'):
        lib.pardox_series_sub.argtypes = [c_void_p, c_char_p, c_void_p, c_char_p]
        lib.pardox_series_sub.restype = c_void_p
        
    # MUL
    if hasattr(lib, 'pardox_series_mul'):
        lib.pardox_series_mul.argtypes = [c_void_p, c_char_p, c_void_p, c_char_p]
        lib.pardox_series_mul.restype = c_void_p

    # DIV
    if hasattr(lib, 'pardox_series_div'):
        lib.pardox_series_div.argtypes = [c_void_p, c_char_p, c_void_p, c_char_p]
        lib.pardox_series_div.restype = c_void_p

    # MOD
    if hasattr(lib, 'pardox_series_mod'):
        lib.pardox_series_mod.argtypes = [c_void_p, c_char_p, c_void_p, c_char_p]
        lib.pardox_series_mod.restype = c_void_p

except AttributeError:
    print("Warning: Arithmetic API functions missing (Update Rust Core).")


# -- INIT HANDSHAKE (Memory Safety) --
try:
    lib.pardox_init_engine.argtypes = []
    lib.pardox_init_engine.restype = None
    
    # WARMUP CALL
    lib.pardox_init_engine() 
    # print(f"✅ PardoX Core loaded from: {lib_path}") 
except AttributeError:
    pass

# =========================================================================
# API 28: FILTER PREDICATES
# =========================================================================
try:
    # Column vs Column
    if hasattr(lib, 'pardox_filter_compare'):
        lib.pardox_filter_compare.argtypes = [c_void_p, c_char_p, c_void_p, c_char_p, c_int32]
        lib.pardox_filter_compare.restype = c_void_p

    # Column vs Scalar
    if hasattr(lib, 'pardox_filter_compare_scalar'):
        lib.pardox_filter_compare_scalar.argtypes = [
            c_void_p, c_char_p, 
            c_double, c_longlong, c_int32, # val_f64, val_i64, is_float
            c_int32 # op_code
        ]
        lib.pardox_filter_compare_scalar.restype = c_void_p
except Exception as e:
    print(f"[PardoX Wrapper Warning] Filter APIs missing: {e}")

# =========================================================================
# API 29: AGGREGATIONS
# =========================================================================
try:
    agg_funcs = [
        'pardox_agg_sum', 'pardox_agg_mean', 'pardox_agg_min', 
        'pardox_agg_max', 'pardox_agg_count', 'pardox_agg_std'
    ]
    for func in agg_funcs:
        if hasattr(lib, func):
            getattr(lib, func).argtypes = [c_void_p, c_char_p]
            getattr(lib, func).restype = c_double
except Exception as e:
    print(f"[PardoX Wrapper Warning] Aggregation APIs missing: {e}")

# =========================================================================
# API 30: APPLY FILTER
# =========================================================================
try:
    if hasattr(lib, 'pardox_apply_filter'):
        lib.pardox_apply_filter.argtypes = [c_void_p, c_void_p, c_char_p]
        lib.pardox_apply_filter.restype = c_void_p
except Exception:
    pass

# =========================================================================
# API Writers 6: PERSISTENCE (WRITERS)
# =========================================================================
try:
    # CSV Writer (Defined in api_writers.rs)
    if hasattr(lib, 'pardox_to_csv'):
        # Args: (ManagerPtr, FilePath)
        lib.pardox_to_csv.argtypes = [c_void_p, c_char_p]
        # Returns: 1 on Success, Negative on Error
        lib.pardox_to_csv.restype = c_longlong

    # PRDX Writer (Binary Dump - Native)
    if hasattr(lib, 'pardox_to_prdx'):
        lib.pardox_to_prdx.argtypes = [c_void_p, c_char_p]
        lib.pardox_to_prdx.restype = c_longlong

except Exception as e:
    print(f"[PardoX Wrapper Warning] Writer APIs missing: {e}")

# =========================================================================
# API 7: NATIVE READERS (INSPECTION) - DEFINED IN api_reader.rs
# =========================================================================
try:
    # Read Head (returns JSON String ptr)
    # fn pardox_read_head_json(path: *const c_char, limit: usize) -> *mut c_char
    if hasattr(lib, 'pardox_read_head_json'):
        lib.pardox_read_head_json.argtypes = [c_char_p, c_size_t]
        lib.pardox_read_head_json.restype = c_char_p

    # Column Sum (Benchmark / Integrity)
    # fn pardox_column_sum(path: *const c_char, col: *const c_char) -> c_double
    if hasattr(lib, 'pardox_column_sum'):
        lib.pardox_column_sum.argtypes = [c_char_p, c_char_p]
        lib.pardox_column_sum.restype = c_double

except Exception as e:
    print(f"[PardoX Wrapper Warning] Native Reader APIs missing: {e}")

# =========================================================================
# API 8: MUTATION & COMPUTE KERNELS (Defined in api_core.rs)
# =========================================================================
try:
    # 1. Column Assignment (In-Place Mutation)
    # fn pardox_add_column(target: *mut Mgr, source: *mut Mgr, name: *const char) -> c_longlong
    if hasattr(lib, 'pardox_add_column'):
        lib.pardox_add_column.argtypes = [c_void_p, c_void_p, c_char_p]
        lib.pardox_add_column.restype = c_longlong

    # 2. Fill Nulls (Data Cleaning)
    # fn pardox_fill_na(mgr: *mut Mgr, col: *const char, val: c_double) -> c_longlong
    if hasattr(lib, 'pardox_fill_na'):
        lib.pardox_fill_na.argtypes = [c_void_p, c_char_p, c_double]
        lib.pardox_fill_na.restype = c_longlong

    # 3. Rounding (Data Transformation)
    # fn pardox_round(mgr: *mut Mgr, col: *const char, decimals: c_int) -> c_longlong
    if hasattr(lib, 'pardox_round'):
        lib.pardox_round.argtypes = [c_void_p, c_char_p, c_int32]
        lib.pardox_round.restype = c_longlong

except Exception as e:
    print(f"[PardoX Wrapper Warning] Compute/Mutation APIs missing: {e}")

# =========================================================================
# API 9: MEMORY INGESTION (Dummy Data / JSON Bridge)
# =========================================================================
try:
    # Ingest JSON Bytes directly from Memory
    # fn pardox_read_json_bytes(json_bytes: *const u8, len: usize) -> *mut HyperBlockManager
    if hasattr(lib, 'pardox_read_json_bytes'):
        lib.pardox_read_json_bytes.argtypes = [c_char_p, c_size_t]
        # CRITICAL CHANGE: Returns a POINTER to the new Manager, not an ID.
        lib.pardox_read_json_bytes.restype = c_void_p 

except Exception as e:
    print(f"[PardoX Wrapper Warning] Memory Ingestion APIs missing: {e}")