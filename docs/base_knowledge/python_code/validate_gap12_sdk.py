#!/usr/bin/env python3
"""
Gap 12 - Universal PRDX Loader (SDK version)
=============================================
Validates pdx.load_prdx() which loads a .prdx binary file into a full
in-memory DataFrame, enabling all compute operations (groupby, str_upper, etc.)
to run on PRDX data through the high-level Python SDK.

Dataset: ventas_consolidado.prdx
  Expected columns: transaction_id, client_id, date_time, entity, category,
                    client_segment, amount, tax_rate
"""

import sys
import os
import pardox as pdx

PRDX = "ventas_consolidado.prdx"
EXPECTED_COLS = {
    "transaction_id", "client_id", "date_time", "entity",
    "category", "client_segment", "amount", "tax_rate",
}
LIMIT = 10_000

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
# Load PRDX file
# ---------------------------------------------------------------------------
section(f"SETUP: Load ventas_consolidado.prdx (limit={LIMIT:,})")
prdx_size_gb = os.path.getsize(PRDX) / 1e9 if os.path.exists(PRDX) else 0
print(f"  File: {PRDX}")
print(f"  Size: {prdx_size_gb:.2f} GB")

df = None
try:
    df = pdx.load_prdx(PRDX, limit=LIMIT)
    print(f"  Loaded: {df.shape[0]:,} rows, {df.shape[1]} cols")
    print(f"  Columns: {df.columns}")
except Exception as e:
    print(f"  ERROR loading PRDX: {e}")

# ---------------------------------------------------------------------------
# TEST 1 - load_prdx returns a DataFrame
# ---------------------------------------------------------------------------
section("TEST 1 - load_prdx returns a DataFrame (non-null)")
ok("load_prdx returns DataFrame instance",
   df is not None and isinstance(df, pdx.DataFrame),
   f"type={type(df).__name__}")

# ---------------------------------------------------------------------------
# TEST 2 - row count == LIMIT
# ---------------------------------------------------------------------------
section("TEST 2 - row count matches limit")
if df is not None:
    ok("row count == 10,000",
       df.shape[0] == LIMIT,
       f"got {df.shape[0]:,}")
else:
    ok("row count", False, "df is None")

# ---------------------------------------------------------------------------
# TEST 3 - schema has expected columns
# ---------------------------------------------------------------------------
section("TEST 3 - schema has all 8 expected columns")
if df is not None:
    found_cols = set(df.columns)
    missing    = EXPECTED_COLS - found_cols
    ok("all 8 expected columns present",
       len(missing) == 0,
       f"found={sorted(found_cols)}" if not missing else f"MISSING: {missing}")
else:
    ok("schema check", False, "df is None")

# ---------------------------------------------------------------------------
# TEST 4 - groupby on loaded PRDX data
# ---------------------------------------------------------------------------
section("TEST 4 - groupby category -> sum(amount) on PRDX data")
if df is not None:
    try:
        result = df.groupby("category", {"amount": "sum"})
        ok("groupby returns DataFrame with groups > 0",
           isinstance(result, pdx.DataFrame) and result.shape[0] > 0,
           f"{result.shape[0]} categories")
    except Exception as e:
        ok("groupby on PRDX", False, str(e))
else:
    ok("groupby on PRDX", False, "df is None")

# ---------------------------------------------------------------------------
# TEST 5 - str_upper on a string column
# ---------------------------------------------------------------------------
section("TEST 5 - str_upper on 'entity' column")
if df is not None:
    try:
        result = df.str_upper("entity")
        ok("str_upper returns DataFrame with same row count",
           isinstance(result, pdx.DataFrame) and result.shape[0] == LIMIT,
           f"rows={result.shape[0]:,}")
    except Exception as e:
        ok("str_upper on PRDX", False, str(e))
else:
    ok("str_upper on PRDX", False, "df is None")

# ---------------------------------------------------------------------------
# TEST 6 - invalid path raises FileNotFoundError
# ---------------------------------------------------------------------------
section("TEST 6 - invalid path raises proper error")
try:
    bad = pdx.load_prdx("/nonexistent/path/file.prdx")
    ok("invalid path should raise error", False, "no exception raised")
except FileNotFoundError:
    ok("invalid path raises FileNotFoundError", True)
except RuntimeError:
    ok("invalid path raises RuntimeError", True)
except Exception as e:
    ok("invalid path raises exception", True, f"got {type(e).__name__}: {e}")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n{'=' * 60}")
print(f"  Gap 12 - Universal PRDX Loader (SDK): RESULTS")
print(f"{'=' * 60}")
print(f"  RESULTS: {passed} passed, {failed} failed")
print(f"{'=' * 60}")

if failed > 0:
    print(f"\n  SOME TESTS FAILED ({failed})")
    sys.exit(1)
else:
    print(f"\n  ALL {passed} TESTS PASSED")
    sys.exit(0)
