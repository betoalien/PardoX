#!/usr/bin/env python3
"""
Gap 2 - String & Date Functions Validation
===========================================
Validates str_upper, str_lower, str_len, str_trim, str_contains, str_replace
and date_extract, date_format, date_add, date_diff through the Python SDK.
"""

import sys

import pardox as pdx

SALES = "sales.csv"
CUST  = "customers.csv"

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
df_cust  = pdx.read_csv(CUST)
ok("sales loaded",     df_sales.shape[0] == 100000)
ok("customers loaded", df_cust.shape[0]  == 50000)

# ---------------------------------------------------------------------------
print("\n=== STRING: str_upper(first_name) ===")
try:
    result = df_cust.str_upper("first_name")
    print(result.head())
    ok("str_upper returns DataFrame", result.shape[0] > 0)
except Exception as e:
    ok("str_upper", False, str(e))

print("\n=== STRING: str_lower(state) ===")
try:
    result = df_cust.str_lower("state")
    print(result.head())
    ok("str_lower returns DataFrame", result.shape[0] > 0)
except Exception as e:
    ok("str_lower", False, str(e))

print("\n=== STRING: str_len(first_name) ===")
try:
    result = df_cust.str_len("first_name")
    print(result.head())
    ok("str_len returns DataFrame", result.shape[0] > 0)
except Exception as e:
    ok("str_len", False, str(e))

print("\n=== STRING: str_contains(category, 'Electronics') ===")
try:
    result = df_sales.str_contains("category", "Electronics")
    print(result.head())
    ok("str_contains returns DataFrame", result.shape[0] > 0)
except Exception as e:
    ok("str_contains", False, str(e))

print("\n=== STRING: str_replace(email, '@example.com', '@pardox.io') ===")
try:
    result = df_cust.str_replace("email", "@example.com", "@pardox.io")
    print(result.head())
    ok("str_replace returns DataFrame", result.shape[0] > 0)
except Exception as e:
    ok("str_replace", False, str(e))

print("\n=== STRING: str_trim(city) ===")
try:
    result = df_cust.str_trim("city")
    print(result.head())
    ok("str_trim returns DataFrame", result.shape[0] > 0)
except Exception as e:
    ok("str_trim", False, str(e))

# ---------------------------------------------------------------------------
# Date functions - customers.registration_date is text "YYYY-MM-DD" in the CSV
# We need a dataset with Int64 date (days since epoch) for date functions
# Let's create a small in-memory DataFrame with epoch-day values
print("\n=== DATE: Build date test DataFrame ===")
from datetime import date

def epoch_days(s):
    return (date.fromisoformat(s) - date(1970, 1, 1)).days

records = [
    {"name": "Alice",   "date_a": epoch_days("2025-07-22"), "date_b": epoch_days("2025-01-01")},
    {"name": "Bob",     "date_a": epoch_days("2022-04-27"), "date_b": epoch_days("2022-01-01")},
    {"name": "Charlie", "date_a": epoch_days("2023-10-15"), "date_b": epoch_days("2023-01-01")},
    {"name": "Dave",    "date_a": epoch_days("2024-01-01"), "date_b": epoch_days("2024-01-01")},
    {"name": "Eve",     "date_a": epoch_days("2021-06-30"), "date_b": epoch_days("2021-01-01")},
]
df_dates = pdx.DataFrame(records)
print(df_dates)
ok("date test DataFrame created", df_dates.shape == (5, 3))

print("\n=== DATE: date_extract(date_a, 'year') ===")
try:
    result = df_dates.date_extract("date_a", "year")
    print(result)
    ok("date_extract year returns DataFrame", result.shape[0] == 5)
except Exception as e:
    ok("date_extract year", False, str(e))

print("\n=== DATE: date_extract(date_a, 'month') ===")
try:
    result = df_dates.date_extract("date_a", "month")
    print(result)
    ok("date_extract month returns DataFrame", result.shape[0] == 5)
except Exception as e:
    ok("date_extract month", False, str(e))

print("\n=== DATE: date_format(date_a, '%Y-%m-%d') ===")
try:
    result = df_dates.date_format("date_a", "%Y-%m-%d")
    print(result)
    ok("date_format returns DataFrame", result.shape[0] == 5)
except Exception as e:
    ok("date_format", False, str(e))

print("\n=== DATE: date_add(date_a, +30) ===")
try:
    result = df_dates.date_add("date_a", 30)
    print(result)
    ok("date_add returns DataFrame", result.shape[0] == 5)
except Exception as e:
    ok("date_add", False, str(e))

print("\n=== DATE: date_diff(date_a, date_b, 'days') ===")
try:
    result = df_dates.date_diff("date_a", "date_b", "days")
    print(result)
    ok("date_diff days returns DataFrame", result.shape[0] == 5)
except Exception as e:
    ok("date_diff days", False, str(e))

# ---------------------------------------------------------------------------
print(f"\n{'='*50}")
print(f"  RESULTS: {passed} passed, {failed} failed")
if failed == 0:
    print("  ALL TESTS PASSED - Gap 2 String & Date ✓")
else:
    print(f"  {failed} FAILED")
    sys.exit(1)
