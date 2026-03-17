#!/usr/bin/env python3
"""
validate_gap21_sdk.py - Gap 21: Arrow Flight Server (TCP + Arrow IPC)

Tests the Arrow Flight server through the Python SDK only.

Uses ONLY:
    import pardox as pdx
    pdx.flight_start(port)      - start local Flight server
    pdx.flight_register(name, df) - register a DataFrame
    pdx.flight_read(host, port, name) - read back a DataFrame
    pdx.flight_stop()           - stop the Flight server
    pdx.read_csv(path)          - load CSV
    df.to_prdx(path)            - save as .prdx
    df.shape, df.columns        - DataFrame properties

Tests:
    1  - SDK exposes flight_start, flight_register, flight_read, flight_stop
    2  - pdx.read_csv loads sales.csv successfully (rows > 0)
    3  - flight_start returns non-negative value (server starts or already started)
    4  - flight_register returns non-negative on success
    5  - flight_read returns a DataFrame (round-trip succeeds)
    6  - flight_stop returns without error
"""

import sys

import pardox as pdx

import os
import tempfile
import time

# ---------------------------------------------------------------------------
# Data paths
# ---------------------------------------------------------------------------
SALES_CSV = "sales.csv"
FLIGHT_PORT = 18888

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------
passed = failed = 0


def ok(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  [OK]   {name}" + (f"  - {detail}" if detail else ""))
    else:
        failed += 1
        print(f"  [FAIL] {name}" + (f"  - {detail}" if detail else ""))


# ---------------------------------------------------------------------------
print("\n=== Gap 21 - Arrow Flight Server (TCP + Arrow IPC) ===\n")

# ---------------------------------------------------------------------------
# Test 1: SDK exports
# ---------------------------------------------------------------------------
print("=== SDK EXPORTS ===")
ok("SDK exposes flight_start",    callable(getattr(pdx, 'flight_start',    None)))
ok("SDK exposes flight_register", callable(getattr(pdx, 'flight_register', None)))
ok("SDK exposes flight_read",     callable(getattr(pdx, 'flight_read',     None)))
ok("SDK exposes flight_stop",     callable(getattr(pdx, 'flight_stop',     None)))

# ---------------------------------------------------------------------------
# Test 2: Load sales.csv
# ---------------------------------------------------------------------------
print("\n=== DATA LOADING ===")
df = None
try:
    df = pdx.read_csv(SALES_CSV)
    rows, cols = df.shape
    ok("read_csv loads sales.csv", rows > 0, f"rows={rows}, cols={cols}")
except Exception as e:
    ok("read_csv loads sales.csv", False, str(e))

if df is None:
    print("[FATAL] Could not load sales.csv - cannot continue Flight tests.")
    print(f"\n{'='*50}")
    print(f"  RESULTS: {passed} passed, {failed} failed")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Save to temp .prdx (required for registration)
# ---------------------------------------------------------------------------
tmp_dir = tempfile.mkdtemp(prefix='pardox_gap21_sdk_')
tmp_prdx = os.path.join(tmp_dir, 'sales_flight.prdx')
try:
    df.to_prdx(tmp_prdx)
    ok("df.to_prdx saves file to disk", os.path.exists(tmp_prdx),
       f"path={tmp_prdx}")
except Exception as e:
    ok("df.to_prdx saves file to disk", False, str(e))

# ---------------------------------------------------------------------------
# Test 3: Start Flight server
# ---------------------------------------------------------------------------
print("\n=== FLIGHT SERVER ===")
start_result = None
try:
    start_result = pdx.flight_start(FLIGHT_PORT)
    ok("flight_start returns non-negative (server started)",
       isinstance(start_result, int) and start_result >= 0,
       f"ret={start_result}")
    time.sleep(0.1)
except NotImplementedError as e:
    ok("flight_start returns non-negative (server started)", False,
       f"NotImplemented: {e}")
except Exception as e:
    ok("flight_start returns non-negative (server started)", False, str(e))

# ---------------------------------------------------------------------------
# Test 4: Register DataFrame on Flight server
# ---------------------------------------------------------------------------
register_ok = False
try:
    reg_result = pdx.flight_register("sales_flight", df)
    register_ok = isinstance(reg_result, int) and reg_result >= 0
    ok("flight_register returns non-negative", register_ok, f"ret={reg_result}")
except NotImplementedError as e:
    ok("flight_register returns non-negative", False, f"NotImplemented: {e}")
except Exception as e:
    ok("flight_register returns non-negative", False, str(e))

# ---------------------------------------------------------------------------
# Test 5: Read back via flight_read (round-trip)
# ---------------------------------------------------------------------------
rt_df = None
try:
    rt_df = pdx.flight_read("127.0.0.1", FLIGHT_PORT, "sales_flight")
    rt_rows, rt_cols = rt_df.shape
    orig_rows, orig_cols = df.shape
    ok("flight_read returns a DataFrame with rows > 0",
       rt_rows > 0,
       f"orig={orig_rows} rt={rt_rows}, cols={rt_cols}")
except NotImplementedError as e:
    ok("flight_read returns a DataFrame with rows > 0", False,
       f"NotImplemented: {e}")
except RuntimeError as e:
    # Server may not be fully functional - treat as graceful failure
    ok("flight_read returns a DataFrame with rows > 0", False,
       f"RuntimeError (server may not be running): {e}")
except Exception as e:
    ok("flight_read returns a DataFrame with rows > 0", False, str(e))

# ---------------------------------------------------------------------------
# Test 6: Stop Flight server
# ---------------------------------------------------------------------------
print("\n=== FLIGHT SHUTDOWN ===")
try:
    stop_result = pdx.flight_stop()
    ok("flight_stop completes without exception",
       True,
       f"ret={stop_result}")
except NotImplementedError as e:
    ok("flight_stop completes without exception", False,
       f"NotImplemented: {e}")
except Exception as e:
    ok("flight_stop completes without exception", False, str(e))

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
import shutil
shutil.rmtree(tmp_dir, ignore_errors=True)

# ---------------------------------------------------------------------------
print(f"\n{'='*50}")
print(f"  RESULTS: {passed} passed, {failed} failed")
if failed == 0:
    print("  ALL TESTS PASSED - Gap 21 Arrow Flight ✓")
else:
    print(f"  {failed} FAILED")
    sys.exit(1)
