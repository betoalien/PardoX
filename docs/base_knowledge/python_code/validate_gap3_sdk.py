#!/usr/bin/env python3
"""
Gap 3 - Decimal Type Validation
"""
import sys
import pardox as pdx

SALES = "sales.csv"
passed = failed = 0

def ok(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  [OK]   {name}" + (f"  - {detail}" if detail else ""))
    else:
        failed += 1
        print(f"  [FAIL] {name}" + (f"  - {detail}" if detail else ""))

print("\n=== LOAD DATA ===")
df = pdx.read_csv(SALES)
print(df.head())
ok("sales loaded", df.shape[0] == 100000)

print("\n=== decimal_from_float(price, scale=2) ===")
try:
    result = df.decimal_from_float("price", 2)
    print(result.head())
    ok("decimal_from_float returns DataFrame", result.shape[0] > 0)
    ok("same row count", result.shape[0] == df.shape[0])
except Exception as e:
    ok("decimal_from_float", False, str(e))

print("\n=== decimal_round(price, scale=0) ===")
try:
    dec_df  = df.decimal_from_float("price", 2)
    rounded = dec_df.decimal_round("price", 0)
    print(rounded.head())
    ok("decimal_round returns DataFrame", rounded.shape[0] > 0)
except Exception as e:
    ok("decimal_round", False, str(e))

print("\n=== decimal_to_float(price) ===")
try:
    dec_df = df.decimal_from_float("price", 2)
    back   = dec_df.decimal_to_float("price")
    print(back.head())
    ok("decimal_to_float returns DataFrame", back.shape[0] > 0)
except Exception as e:
    ok("decimal_to_float", False, str(e))

print("\n=== decimal_mul_float(price, 1.1) ===")
try:
    dec_df = df.decimal_from_float("price", 2)
    result = dec_df.decimal_mul_float("price", 1.1)
    print(result.head())
    ok("decimal_mul_float returns DataFrame", result.shape[0] > 0)
except Exception as e:
    ok("decimal_mul_float", False, str(e))

print("\n=== decimal_sum(price) ===")
try:
    dec_df = df.decimal_from_float("price", 2)
    total  = dec_df.decimal_sum("price")
    print(f"  Total price: {total:.2f}")
    ok("decimal_sum returns float > 0", isinstance(total, float) and total > 0, f"{total:.2f}")
except Exception as e:
    ok("decimal_sum", False, str(e))

print(f"\n{'='*50}")
print(f"  RESULTS: {passed} passed, {failed} failed")
if failed == 0:
    print("  ALL TESTS PASSED - Gap 3 Decimal ✓")
else:
    print(f"  {failed} FAILED")
    sys.exit(1)
