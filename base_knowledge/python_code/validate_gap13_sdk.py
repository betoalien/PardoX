#!/usr/bin/env python3
"""
Gap 13 - Streaming GroupBy over .prdx (SDK version)
=====================================================
Validates the lazy pipeline (Gap 5 / Gap 27) and streaming-style operations
over PRDX files through the high-level Python SDK.

Gap 13 streaming GroupBy (pardox_prdx_*) operates directly on the .prdx binary
without loading the full file into RAM.  In the SDK layer, the equivalent
entry-point is the LazyFrame pipeline (scan_prdx / scan_csv -> filter -> collect),
which also avoids eager full-load.

This script validates:
  1. pdx.scan_csv returns a LazyFrame (not None)
  2. LazyFrame type is pdx.LazyFrame
  3. scan_csv + collect materialises a DataFrame
  4. scan_csv + filter + collect works (lazy predicate pushdown)
  5. pdx.scan_prdx returns a LazyFrame (not None)
"""

import sys
import pardox as pdx

SALES = "sales.csv"
PRDX  = "ventas_consolidado.prdx"

# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------
passed = failed = 0

def ok(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  [OK]   {name}" + (f"  - {detail}" if detail else ""))
    else:
        failed += 1
        print(f"  [FAIL] {name}" + (f"  - {detail}" if detail else ""))

def section(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")

# ---------------------------------------------------------------------------
# TEST 1 - scan_csv returns a LazyFrame
# ---------------------------------------------------------------------------
section("TEST 1 - pdx.scan_csv returns a LazyFrame (no data loaded yet)")
lf = None
try:
    lf = pdx.scan_csv(SALES)
    ok("scan_csv returns LazyFrame instance",
       lf is not None and isinstance(lf, pdx.LazyFrame),
       f"type={type(lf).__name__}")
except Exception as e:
    ok("scan_csv returns LazyFrame", False, str(e))

# ---------------------------------------------------------------------------
# TEST 2 - LazyFrame type
# ---------------------------------------------------------------------------
section("TEST 2 - LazyFrame is the correct type (pdx.LazyFrame)")
ok("LazyFrame is pdx.LazyFrame",
   lf is not None and type(lf).__name__ == "LazyFrame",
   f"type={type(lf).__name__ if lf else 'None'}")

# ---------------------------------------------------------------------------
# TEST 3 - scan_csv + collect materialises a full DataFrame
# ---------------------------------------------------------------------------
section("TEST 3 - scan_csv + collect materialises DataFrame")
try:
    lf_collect = pdx.scan_csv(SALES)
    df = lf_collect.collect()
    ok("collect returns DataFrame with rows > 0",
       isinstance(df, pdx.DataFrame) and df.shape[0] > 0,
       f"shape={df.shape}")
except Exception as e:
    ok("scan_csv + collect", False, str(e))

# ---------------------------------------------------------------------------
# TEST 4 - scan_csv + filter + collect (lazy predicate pushdown)
# ---------------------------------------------------------------------------
section("TEST 4 - scan_csv + filter('price','gt',0) + collect")
try:
    lf_filt = pdx.scan_csv(SALES)
    lf_filt = lf_filt.filter("price", "gt", 0.0)
    df_filt = lf_filt.collect()
    ok("filter + collect returns DataFrame with rows > 0",
       isinstance(df_filt, pdx.DataFrame) and df_filt.shape[0] > 0,
       f"shape={df_filt.shape}")
except Exception as e:
    ok("scan_csv + filter + collect", False, str(e))

# ---------------------------------------------------------------------------
# TEST 5 - scan_prdx returns a LazyFrame
# ---------------------------------------------------------------------------
section("TEST 5 - pdx.scan_prdx returns a LazyFrame (no full load)")
try:
    lf_prdx = pdx.scan_prdx(PRDX)
    ok("scan_prdx returns LazyFrame instance",
       lf_prdx is not None and isinstance(lf_prdx, pdx.LazyFrame),
       f"type={type(lf_prdx).__name__}")
except NotImplementedError as e:
    ok("scan_prdx API available", False, f"NotImplementedError: {e}")
except Exception as e:
    ok("scan_prdx returns LazyFrame", False, str(e))

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n{'=' * 60}")
print(f"  Gap 13 - Streaming GroupBy / Lazy Pipeline (SDK): RESULTS")
print(f"{'=' * 60}")
print(f"  RESULTS: {passed} passed, {failed} failed")
print(f"  NOTE: Gap 13 streaming scalars (pardox_prdx_min/max/mean/count)")
print(f"        are validated via the raw-FFI script (validate_gap13.py).")
print(f"        This SDK script validates the lazy pipeline entry-point.")
print(f"{'=' * 60}")

if failed > 0:
    print(f"\n  SOME TESTS FAILED ({failed})")
    sys.exit(1)
else:
    print(f"\n  ALL {passed} TESTS PASSED")
    sys.exit(0)
