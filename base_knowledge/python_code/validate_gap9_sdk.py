#!/usr/bin/env python3
"""Gap 9 - Time Series Fill Validation"""
import sys
import pardox as pdx

passed = failed = 0
def ok(name, cond, detail=""):
    global passed, failed
    if cond: passed += 1; print(f"  [OK]   {name}" + (f"  - {detail}" if detail else ""))
    else: failed += 1; print(f"  [FAIL] {name}" + (f"  - {detail}" if detail else ""))

# Create a DataFrame with None/NaN-like data (using large sentinel values in CSV)
import tempfile, os, csv
rows = [{"id": i, "value": i * 10.0 if i % 3 != 0 else None} for i in range(1, 21)]
# Write to temp CSV omitting None values as empty string
tmpf = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
writer = csv.DictWriter(tmpf, fieldnames=["id", "value"])
writer.writeheader()
for r in rows:
    writer.writerow({"id": r["id"], "value": r["value"] if r["value"] is not None else ""})
tmpf.close()

SALES = "sales.csv"
print("\n=== LOAD DATA ===")
df = pdx.read_csv(SALES)
ok("sales loaded", df.shape[0] == 100000)

print("\n=== ffill(price) ===")
try:
    result = df.ffill("price")
    print(result.head())
    ok("ffill returns DataFrame", result.shape[0] > 0, f"{result.shape[0]} rows")
except Exception as e:
    ok("ffill", False, str(e))

print("\n=== bfill(price) ===")
try:
    result = df.bfill("price")
    print(result.head())
    ok("bfill returns DataFrame", result.shape[0] > 0)
except Exception as e:
    ok("bfill", False, str(e))

print("\n=== interpolate(price) ===")
try:
    result = df.interpolate("price")
    print(result.head())
    ok("interpolate returns DataFrame", result.shape[0] > 0)
except Exception as e:
    ok("interpolate", False, str(e))

os.unlink(tmpf.name)
print(f"\n{'='*50}")
print(f"  RESULTS: {passed} passed, {failed} failed")
if failed == 0: print("  ALL TESTS PASSED - Gap 9 Time Series Fill ✓")
else: print(f"  {failed} FAILED"); sys.exit(1)
