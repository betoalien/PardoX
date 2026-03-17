#!/usr/bin/env python3
"""Gap 14 - SQL over DataFrame Validation"""
import sys
import pardox as pdx

SALES = "sales.csv"
passed = failed = 0
def ok(name, cond, detail=""):
    global passed, failed
    if cond: passed += 1; print(f"  [OK]   {name}" + (f"  - {detail}" if detail else ""))
    else: failed += 1; print(f"  [FAIL] {name}" + (f"  - {detail}" if detail else ""))

print("\n=== LOAD DATA ===")
df = pdx.read_csv(SALES)
ok("sales loaded", df.shape[0] == 100000)

print("\n=== df.sql('SELECT category, SUM(price) FROM df GROUP BY category') ===")
try:
    result = df.sql("SELECT category, SUM(price) FROM df GROUP BY category")
    print(result)
    ok("sql groupby returns DataFrame", result.shape[0] > 0, f"{result.shape[0]} rows")
    ok("sql groupby returns 8 categories", result.shape[0] == 8, f"got {result.shape[0]}")
except Exception as e:
    ok("sql groupby", False, str(e))

print("\n=== df.sql('SELECT * FROM df WHERE price > 500 LIMIT 10') ===")
try:
    result = df.sql("SELECT * FROM df WHERE price > 500 LIMIT 10")
    print(result)
    ok("sql filter + limit returns DataFrame", result.shape[0] > 0, f"{result.shape[0]} rows")
    ok("sql limit keeps at most 10 rows", result.shape[0] <= 10, f"got {result.shape[0]}")
except Exception as e:
    ok("sql filter + limit", False, str(e))

print("\n=== df.sql('SELECT COUNT(*) as cnt FROM df') ===")
try:
    result = df.sql("SELECT COUNT(*) as cnt FROM df")
    print(result)
    ok("sql COUNT returns DataFrame", result.shape[0] > 0)
except Exception as e:
    ok("sql COUNT", False, str(e))

print(f"\n{'='*50}")
print(f"  RESULTS: {passed} passed, {failed} failed")
if failed == 0: print("  ALL TESTS PASSED - Gap 14 SQL over DataFrame ✓")
else: print(f"  {failed} FAILED"); sys.exit(1)
