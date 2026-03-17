#!/usr/bin/env python3
"""
Gap 1 - GroupBy Validation
===========================
Validates the GroupBy API through the high-level Python SDK.
"""

import sys

import pardox as pdx

SALES  = "sales.csv"
CUST   = "customers.csv"

passed = failed = 0

def ok(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  [OK]   {name}" + (f"  - {detail}" if detail else ""))
    else:
        failed += 1
        print(f"  [FAIL] {name}" + (f"  - {detail}" if detail else ""))

# ---------------------------------------------------------------------------
print("\n=== LOAD DATA ===")
df_sales = pdx.read_csv(SALES)
print(df_sales)
ok("sales loaded", df_sales.shape[0] == 100000, f"{df_sales.shape[0]} rows")

df_cust = pdx.read_csv(CUST)
ok("customers loaded", df_cust.shape[0] == 50000, f"{df_cust.shape[0]} rows")

# ---------------------------------------------------------------------------
print("\n=== GroupBy: category -> sum(price), mean(quantity) ===")
try:
    result = df_sales.groupby("category", {"price": "sum", "quantity": "mean"})
    print(result)
    ok("groupby returns DataFrame", result.shape[0] > 0, f"{result.shape[0]} groups")
    ok("groupby has 8 categories", result.shape[0] == 8, f"got {result.shape[0]}")
except Exception as e:
    ok("groupby category", False, str(e))

# ---------------------------------------------------------------------------
print("\n=== GroupBy: id_customer -> sum(price), count ===")
try:
    result2 = df_sales.groupby("id_customer", {"price": "sum", "id_transaction": "count"})
    print(result2.head())
    ok("groupby id_customer returns result", result2.shape[0] > 0, f"{result2.shape[0]} groups")
    ok("groupby has many customers", result2.shape[0] > 40000, f"got {result2.shape[0]}")
except Exception as e:
    ok("groupby id_customer", False, str(e))

# ---------------------------------------------------------------------------
print("\n=== GroupBy: state -> count ===")
try:
    result3 = df_cust.groupby("state", {"id_customer": "count"})
    print(result3)
    ok("groupby state returns result", result3.shape[0] > 0, f"{result3.shape[0]} states")
    ok("groupby has 12 states", result3.shape[0] == 12, f"got {result3.shape[0]}")
except Exception as e:
    ok("groupby state", False, str(e))

# ---------------------------------------------------------------------------
print("\n=== Multi-column GroupBy: [state, city] -> count ===")
try:
    result4 = df_cust.groupby(["state", "city"], {"id_customer": "count"})
    print(result4.head())
    ok("multi-col groupby returns result", result4.shape[0] > 0, f"{result4.shape[0]} groups")
    ok("multi-col has more groups than states", result4.shape[0] > 12)
except Exception as e:
    ok("multi-col groupby", False, str(e))

# ---------------------------------------------------------------------------
print("\n=== GroupBy: min / max / std ===")
try:
    result5 = df_sales.groupby("category", {"price": "min", "quantity": "max", "discount": "std"})
    print(result5)
    ok("groupby min/max/std returns result", result5.shape[0] == 8, f"got {result5.shape[0]}")
except Exception as e:
    ok("groupby min/max/std", False, str(e))

# ---------------------------------------------------------------------------
print(f"\n{'='*50}")
print(f"  RESULTS: {passed} passed, {failed} failed")
if failed == 0:
    print("  ALL TESTS PASSED - Gap 1 GroupBy")
else:
    print(f"  {failed} FAILED")
    sys.exit(1)
