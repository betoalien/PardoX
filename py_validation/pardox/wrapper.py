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

# Expose libs/ folder to dynamic linker so the Rust Core can find companion
# plugins (GPU, Server) by their canonical names (e.g. libpardox_gpu.so).
# This must be done BEFORE loading the CPU library.
_gpu_canonical  = os.path.join(lib_folder, "libpardox_gpu.so")
_gpu_actual     = os.path.join(lib_folder, "pardox-gpu-Linux-x64.so")
_srv_canonical  = os.path.join(lib_folder, "libpardox_server.so")
_srv_actual     = os.path.join(lib_folder, "pardox-server-Linux-x64.so")

for _canonical, _actual in [(_gpu_canonical, _gpu_actual), (_srv_canonical, _srv_actual)]:
    if not os.path.exists(_canonical) and os.path.exists(_actual):
        try:
            os.symlink(_actual, _canonical)
        except (OSError, NotImplementedError):
            pass  # Symlink may already exist or not be supported

# Add libs folder to LD_LIBRARY_PATH so dlopen inside the Core finds companions
_existing_ld = os.environ.get("LD_LIBRARY_PATH", "")
if lib_folder not in _existing_ld:
    os.environ["LD_LIBRARY_PATH"] = lib_folder + (":" + _existing_ld if _existing_ld else "")

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

# pardox_free_string(*char) — frees heap-allocated strings from CString::into_raw()
try:
    lib.pardox_free_string.argtypes = [c_void_p]
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

# ── FFI Bindings ─────────────────────────────────────────────────────────────
import ctypes as _ctypes_local
from ._bindings import bind_all
bind_all(lib, c_void_p, c_char_p, c_int32, c_double, c_longlong, c_size_t, c_int64, _ctypes_local.c_int16)

# ── WebAssembly (Gap 17) ─────────────────────────────────────────────────────
wasm = None
_wasm_path = os.path.join(os.path.dirname(__file__), "libs", "Linux", "pardox_wasm.js")
if os.path.exists(_wasm_path):
    try:
        import importlib.util as _ilu
        _spec = _ilu.spec_from_file_location("pardox_wasm", _wasm_path)
        if _spec and _spec.loader:
            _wasm_mod = _ilu.module_from_spec(_spec)
            _spec.loader.exec_module(_wasm_mod)
            wasm = getattr(_wasm_mod, 'PardoxWasm', None)
    except Exception:
        pass

# =========================================================================
# (Legacy binding declarations below have been moved to _bindings/ modules)
# =========================================================================
# PLACEHOLDER — keeping this section marker so file diffs are readable.
# All argtypes/restype declarations are now handled by bind_all() above.
# =========================================================================
if False:  # dead code block — preserves line references for git blame
    pass  # nothing to do
