import os
import glob
import time
import json
import psutil
import platform
import ctypes

# ==============================================================================
# 📦 PARDOX IMPORT (INSTALLED VIA PIP)
# ==============================================================================
try:
    import pardox
    # We access the Core Library exposed by the wrapper
    # This automatically handles .dll (Windows), .so (Linux), or .dylib (Mac)
    from pardox.wrapper import lib, c_char_p, c_void_p, c_longlong
except ImportError:
    print("❌ Error: 'pardox' not found.")
    print("   Please run: pip install pardox")
    raise SystemExit(1)

# ==============================================================================
# ⚠️ DATA REQUIREMENT WARNING
# ==============================================================================
# This benchmark is designed for the "Data Lake Challenge".
# It requires 320 CSV files (~640 Million Rows total) generated previously.
# Files should be named: Ventas_0.csv, Ventas_1.csv ... Ventas_319.csv
# ==============================================================================

# ==============================================================================
# USER CONFIGURATION
# ==============================================================================

# Percentage of RAM dedicated to the HyperBlock Buffer
TARGET_RAM_RATIO = 0.45 

# ==============================================================================
# PATH DEFINITIONS (CROSS-PLATFORM)
# ==============================================================================

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Input: Folder containing the 320 CSV files
DATA_LAKE_DIR = os.path.join(CURRENT_DIR, "data_lake_challenge_640m")
INPUT_PATTERN = os.path.join(DATA_LAKE_DIR, "Ventas_*.csv")

# Output: The consolidated binary file
OUTPUT_DIR = os.path.join(CURRENT_DIR, "final_parquet_massive")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "Ventas_Consolidated_640M.prdx")

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Clean previous run
if os.path.exists(OUTPUT_FILE):
    try:
        os.remove(OUTPUT_FILE)
    except OSError:
        pass

# ==============================================================================
# FFI BINDINGS (Internal API V4.3)
# ==============================================================================
# We need to explicitly define the signatures for the low-level benchmark functions
# that might not be exposed in the high-level Python API yet.

try:
    # 1. Main Pipeline V4.3 (Hyper Copy)
    # fn pardox_hyper_copy_v3(pattern, output, schema, config) -> rows_written
    if hasattr(lib, 'pardox_hyper_copy_v3'):
        lib.pardox_hyper_copy_v3.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p]
        lib.pardox_hyper_copy_v3.restype = c_longlong

    # 2. Get Version
    if hasattr(lib, 'pardox_version'):
        lib.pardox_version.argtypes = []
        lib.pardox_version.restype = c_char_p

    # 3. System Report (Auto-Tuning)
    if hasattr(lib, 'pardox_get_system_report'):
        lib.pardox_get_system_report.argtypes = []
        lib.pardox_get_system_report.restype = c_void_p

    # 4. Memory Cleanup
    if hasattr(lib, 'pardox_free_string'):
        lib.pardox_free_string.argtypes = [c_void_p]
        lib.pardox_free_string.restype = None

except Exception as e:
    print(f"❌ Error binding C functions: {e}")
    raise SystemExit(1)

# ==============================================================================
# TELEMETRY UTILITIES
# ==============================================================================

process = psutil.Process(os.getpid())

def bytes_to_mb(x: int) -> float:
    return x / (1024 * 1024) if x is not None else 0.0

def telemetry_snapshot():
    """Cross-platform memory snapshot."""
    mem = process.memory_info()
    # 'rss' is Resident Set Size (Physical RAM used)
    # On Windows, 'peak_wset' is available; on Linux/Mac we fallback to rss.
    peak = getattr(mem, "peak_wset", mem.rss)
    
    return {
        "rss": mem.rss,
        "threads": process.num_threads(),
        "peak": peak
    }

# ==============================================================================
# AUTO-TUNING HANDSHAKE
# ==============================================================================

system_info = {}
if hasattr(lib, 'pardox_get_system_report'):
    raw_ptr = lib.pardox_get_system_report()
    if raw_ptr:
        report_str = ctypes.cast(raw_ptr, c_char_p).value.decode('utf-8')
        lib.pardox_free_string(raw_ptr) 
        system_info = json.loads(report_str)

OPTIMAL_CHUNK_SIZE_MB = system_info.get("io_chunk_mb", 16)
SYS_PLATFORM = platform.system()

print("-" * 70)
print(f"🧠 PARD0X BRAIN ({SYS_PLATFORM} Detected)")
print(f"   • Core Profile: {system_info.get('profile', 'Standard')}")
print(f"   • Rust Threads: {system_info.get('threads', 'Auto')}")
print(f"   • IO Chunk:     {OPTIMAL_CHUNK_SIZE_MB} MB")
if system_info.get("gpu_detected", False):
    print("   ⚠️  GPU Acceleration: AVAILABLE")
print("-" * 70)

# ==============================================================================
# SCHEMA & CONFIG
# ==============================================================================

STRICT_SCHEMA = [
    {"name": "transaction_id", "type": "Int64"},
    {"name": "client_id",      "type": "Int64"},
    {"name": "date_time",      "type": "Datetime"},
    {"name": "entity",         "type": "String"},
    {"name": "category",       "type": "String"},
    {"name": "client_segment", "type": "String"},
    {"name": "amount",         "type": "Float64"},
    {"name": "tax_rate",       "type": "Float64"},
]

# Map nice names to Rust HyperTypes
HYPER_TYPE_MAP = {
    "Int64": "Int64", "Float64": "Float64", 
    "String": "Utf8", "Datetime": "Timestamp"
}

column_names = [col["name"] for col in STRICT_SCHEMA]
column_types = [HYPER_TYPE_MAP[col["type"]] for col in STRICT_SCHEMA]

HYPER_SCHEMA_JSON = json.dumps({"column_names": column_names, "column_types": column_types})

# Pipeline Configuration
CSV_CONFIG = {
    "delimiter": ord(","),
    "quote_char": ord('"'),
    "has_header": True,
    "chunk_size": OPTIMAL_CHUNK_SIZE_MB * 1024 * 1024,
    "target_ram_ratio": TARGET_RAM_RATIO 
}
CSV_CONFIG_JSON = json.dumps(CSV_CONFIG)

# ==============================================================================
# EXECUTION
# ==============================================================================

# Search for files
files_found = len(glob.glob(INPUT_PATTERN))
version_str = "Unknown"
if hasattr(lib, 'pardox_version'):
    version_str = lib.pardox_version().decode("utf-8", errors="replace")

print(f"🚀 ENGINE VERSION: {version_str}")
print(f"📂 INPUT PATH:     {INPUT_PATTERN}")
print(f"📂 FILES FOUND:    {files_found} (Target: 320)")
print(f"📂 OUTPUT PATH:    {OUTPUT_FILE}")
print("-" * 70)

if files_found == 0:
    print("❌ ERROR: No CSV files found.")
    print(f"   Please generate the data lake in: {DATA_LAKE_DIR}")
    raise SystemExit(1)

print(f"\n🔥 STARTING HYPER STREAM ON {SYS_PLATFORM.upper()}...")

telemetry_before = telemetry_snapshot()
start_time = time.time()

# --- RUN PIPELINE ---
rows_written = lib.pardox_hyper_copy_v3(
    INPUT_PATTERN.encode("utf-8"),
    OUTPUT_FILE.encode("utf-8"),
    HYPER_SCHEMA_JSON.encode("utf-8"),
    CSV_CONFIG_JSON.encode("utf-8"),
)
# --------------------

duration = time.time() - start_time
telemetry_after = telemetry_snapshot()

print("-" * 70)

# ==============================================================================
# RESULTS
# ==============================================================================

if rows_written > 0:
    file_size_mb = 0.0
    if os.path.exists(OUTPUT_FILE):
        file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)

    mb_per_sec = file_size_mb / duration if duration > 0 else 0.0
    rows_per_sec = rows_written / duration if duration > 0 else 0.0

    print("✅ SUCCESS! Pipeline Completed.")
    print(f"🔢 Total Rows:    {rows_written:,}")
    print(f"⏱️  Time Elapsed:  {duration:.4f} s")
    print(f"📦 Output Size:    {file_size_mb:.2f} MB")
    print(f"⚡ Throughput:     {mb_per_sec:.2f} MB/s")
    print(f"🚀 Velocity:       {rows_per_sec:,.0f} rows/s")

else:
    # Error Handling Map
    errors = {
        0: "No files matched or empty data.",
        -1: "Invalid Source Pattern.",
        -2: "Invalid Output Path.",
        -3: "Schema Error.",
        -4: "Config Error.",
        -5: "Directory/Globbing Error.",
        -6: "CSV Parse Error.",
        -7: "Writer Creation Failed.",
        -10: "Channel Error.",
        -11: "Panic in Worker Thread."
    }
    print("❌ ERROR: Pipeline Failed.")
    print(f"   Code: {rows_written}")
    print(f"   Reason: {errors.get(rows_written, 'Unknown Error')}")

print("-" * 70)

# Final Stats
print("🧠 MEMORY TELEMETRY")
print(f"   • RAM Before:   {bytes_to_mb(telemetry_before['rss']):.2f} MB")
print(f"   • RAM After:    {bytes_to_mb(telemetry_after['rss']):.2f} MB")
print(f"   • Peak Usage:   {bytes_to_mb(telemetry_after['peak']):.2f} MB")
print("-" * 70)