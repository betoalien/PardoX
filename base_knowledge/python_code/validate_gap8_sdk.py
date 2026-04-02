#!/usr/bin/env python3
"""Gap 8 - Pivot & Melt Validation"""
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

print("\n=== pivot_table(index='category', columns=... , values='price', agg_func='sum') ===")
try:
    result = df.pivot_table(index="category", columns="category", values="price", agg_func="sum")
    print(result)
    ok("pivot_table returns DataFrame", result.shape[0] > 0)
except Exception as e:
    ok("pivot_table", False, str(e))

print("\n=== melt(id_vars=['category'], value_vars=['price', 'quantity']) ===")
try:
    result = df.melt(
        id_vars=["category"],
        value_vars=["price", "quantity"],
        var_name="metric",
        value_name="value"
    )
    print(result.head())
    ok("melt returns DataFrame", result.shape[0] > 0, f"{result.shape[0]} rows")
    ok("melt doubles rows (price + quantity)", result.shape[0] == df.shape[0] * 2, f"got {result.shape[0]}")
except Exception as e:
    ok("melt", False, str(e))

print(f"\n{'='*50}")
print(f"  RESULTS: {passed} passed, {failed} failed")
if failed == 0: print("  ALL TESTS PASSED - Gap 8 Pivot & Melt ✓")
else: print(f"  {failed} FAILED"); sys.exit(1)
