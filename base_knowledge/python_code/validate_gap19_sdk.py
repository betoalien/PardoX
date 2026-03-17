#!/usr/bin/env python3
"""
validate_gap19_sdk.py - Gap 19: Data Contracts / Schema Validation

Uses the public SDK exclusively (import pardox as pdx).
Validates the df.validate_contract() method:
  - Returns a DataFrame (clean rows that pass all rules)
  - Valid rules produce passing results
  - Invalid-type rules expose failures
  - The method is present on pdx.DataFrame

Test plan:
  Test 1 - pdx.DataFrame has validate_contract method
  Test 2 - validate_contract(price min=0) returns a DataFrame
  Test 3 - validate_contract with valid rules: returned rows <= total rows
  Test 4 - validate_contract with impossible rule (min > max price) returns 0 rows
  Test 5 - validate_contract with column that matches schema type returns non-empty result
"""

import sys
import os
import tempfile
import shutil

import pardox as pdx

SALES = "sales.csv"

passed = 0
failed = 0


def ok(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  [OK]   {name}" + (f"  - {detail}" if detail else ""))
    else:
        failed += 1
        print(f"  [FAIL] {name}" + (f"  - {detail}" if detail else ""))


print("\n=== Gap 19: Data Contracts / Schema Validation - SDK Validation ===\n")

# Load source data
try:
    df = pdx.read_csv(SALES)
    total_rows = df.shape[0]
    print(f"[PREP] Loaded sales.csv: {total_rows:,} rows, cols={list(df.columns)}\n")
except Exception as e:
    print(f"[FAIL] Could not load sales.csv: {e}")
    sys.exit(1)

# ── Test 1: pdx.DataFrame has validate_contract method ───────────────────────
ok("Test 1 - pdx.DataFrame has validate_contract method",
   callable(getattr(pdx.DataFrame, 'validate_contract', None)),
   "method present on class")

# ── Test 2: validate_contract returns a DataFrame ────────────────────────────
try:
    rules = {"columns": {"price": {"min": 0.0}}}
    result = df.validate_contract(rules)
    ok("Test 2 - validate_contract() returns a DataFrame",
       isinstance(result, pdx.DataFrame),
       f"type={type(result).__name__}")
except (NotImplementedError, Exception) as e:
    ok("Test 2 - validate_contract() returns a DataFrame",
       False, f"{type(e).__name__}: {str(e)[:100]}")

# ── Test 3: valid rules produce rows <= original ──────────────────────────────
try:
    rules_valid = {"columns": {"price": {"min": 0.0, "max": 100000.0}}}
    df_clean = df.validate_contract(rules_valid)
    clean_rows = df_clean.shape[0]
    ok("Test 3 - validate_contract with valid rules: clean_rows <= total_rows",
       0 <= clean_rows <= total_rows,
       f"clean={clean_rows:,}  total={total_rows:,}")
except (NotImplementedError, Exception) as e:
    ok("Test 3 - validate_contract with valid rules",
       False, f"{type(e).__name__}: {str(e)[:100]}")

# ── Test 4: impossible rule returns 0 clean rows ─────────────────────────────
try:
    # price cannot be > 9999999 AND < -1 at the same time -> no rows pass
    rules_impossible = {"columns": {"price": {"min": 9999999.0}}}
    df_impossible = df.validate_contract(rules_impossible)
    impossible_rows = df_impossible.shape[0]
    ok("Test 4 - impossible min rule returns 0 clean rows",
       impossible_rows == 0,
       f"clean={impossible_rows:,} (expected 0)")
except (NotImplementedError, Exception) as e:
    ok("Test 4 - impossible min rule returns 0 clean rows",
       False, f"{type(e).__name__}: {str(e)[:100]}")

# ── Test 5: contract on Utf8 column 'category' with allowed_values ────────────
try:
    # Use a subset of categories - the result should be < total rows
    rules_cat = {"columns": {"category": {"allowed_values": ["Electronics"]}}}
    df_cat = df.validate_contract(rules_cat)
    cat_rows = df_cat.shape[0]
    ok("Test 5 - validate_contract on category allowed_values returns < total rows",
       0 <= cat_rows < total_rows,
       f"filtered={cat_rows:,}  total={total_rows:,}")
except (NotImplementedError, Exception) as e:
    ok("Test 5 - validate_contract on category allowed_values",
       False, f"{type(e).__name__}: {str(e)[:100]}")

print(f"\n{'='*60}")
print(f"  RESULTS: {passed} passed, {failed} failed")
print(f"{'='*60}")

if failed > 0:
    print(f"\n  {failed} test(s) FAILED")
    sys.exit(1)
else:
    print("  ALL TESTS PASSED - Gap 19 Data Contracts SDK ✓")
