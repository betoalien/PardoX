#!/usr/bin/env python3
"""
Gap 10 - Nested / Complex Data (SDK version)
=============================================
Validates json_extract, explode, and unnest through the high-level Python SDK.
Uses pdx.DataFrame([...]) to build test data with JSON string columns.
"""

import sys
import os
import tempfile
import pardox as pdx

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
# Build test DataFrames from inline data
# ---------------------------------------------------------------------------
section("SETUP: Create test DataFrames with JSON columns")

# Records with JSON objects in the 'metadata' column
records_objects = [
    {"id": 1, "name": "Alice",   "metadata": '{"city":"New York","age":"30","dept":"Engineering"}'},
    {"id": 2, "name": "Bob",     "metadata": '{"city":"Los Angeles","age":"25"}'},
    {"id": 3, "name": "Charlie", "metadata": '{"city":"Chicago","age":"35","dept":"Marketing"}'},
    {"id": 4, "name": "Diana",   "metadata": '{"city":"Houston","age":"28","dept":"Sales"}'},
    {"id": 5, "name": "Eve",     "metadata": '{"city":"Phoenix","age":"32"}'},
]

# Records with JSON arrays in the 'tags' column
records_arrays = [
    {"id": 1, "name": "Alice",   "tags": '["python","rust","sql"]'},
    {"id": 2, "name": "Bob",     "tags": '["java","go"]'},
    {"id": 3, "name": "Charlie", "tags": '["python"]'},
    {"id": 4, "name": "Diana",   "tags": '["rust","c++","wasm","gpu"]'},
    {"id": 5, "name": "Eve",     "tags": "[]"},
]

# Records with nested JSON objects (dot-notation keys)
records_nested = [
    {"id": 1, "name": "Alice",   "profile": '{"address":{"city":"NYC","zip":"10001"},"score":95}'},
    {"id": 2, "name": "Bob",     "profile": '{"address":{"city":"LA","zip":"90001"},"score":88}'},
    {"id": 3, "name": "Charlie", "profile": '{"address":{"city":"CHI","zip":"60601"},"score":72}'},
]

df_objects = pdx.DataFrame(records_objects)
df_arrays  = pdx.DataFrame(records_arrays)
df_nested  = pdx.DataFrame(records_nested)

print(f"  df_objects: {df_objects.shape} - cols: {df_objects.columns}")
print(f"  df_arrays:  {df_arrays.shape}  - cols: {df_arrays.columns}")
print(f"  df_nested:  {df_nested.shape}  - cols: {df_nested.columns}")

ok("df_objects created (5 rows)", df_objects.shape[0] == 5, f"shape={df_objects.shape}")
ok("df_arrays created (5 rows)",  df_arrays.shape[0] == 5,  f"shape={df_arrays.shape}")
ok("df_nested created (3 rows)",  df_nested.shape[0] == 3,  f"shape={df_nested.shape}")

# ---------------------------------------------------------------------------
# json_extract tests
# ---------------------------------------------------------------------------
section("TEST: json_extract - extract simple key")
try:
    result = df_objects.json_extract("metadata", "city")
    rows, cols = result.shape
    ok("json_extract('city') returns DataFrame",
       rows == 5 and cols >= 1,
       f"shape={result.shape}, cols={result.columns}")
except Exception as e:
    ok("json_extract('city')", False, str(e))

section("TEST: json_extract - extract key missing from some rows")
try:
    result = df_objects.json_extract("metadata", "dept")
    ok("json_extract('dept') returns DataFrame with 5 rows",
       result.shape[0] == 5,
       f"shape={result.shape}")
except Exception as e:
    ok("json_extract('dept')", False, str(e))

section("TEST: json_extract - dot-notation nested key")
try:
    result = df_nested.json_extract("profile", "address.city")
    ok("json_extract dot-notation 'address.city' returns 3 rows",
       result.shape[0] == 3,
       f"shape={result.shape}, cols={result.columns}")
except Exception as e:
    ok("json_extract dot-notation", False, str(e))

section("TEST: json_extract - invalid column raises error")
try:
    result = df_objects.json_extract("nonexistent_col", "city")
    ok("json_extract invalid column should raise", False, "no exception raised")
except (RuntimeError, NotImplementedError, Exception):
    ok("json_extract invalid column raises error", True)

# ---------------------------------------------------------------------------
# explode tests
# ---------------------------------------------------------------------------
section("TEST: explode - expand JSON array column into rows")
try:
    result = df_arrays.explode("tags")
    # 3 + 2 + 1 + 4 + 1 (empty) = 11 rows
    ok("explode('tags') returns DataFrame with expanded rows",
       result.shape[0] >= 10,
       f"shape={result.shape} (expected >=10 from arrays)")
except Exception as e:
    ok("explode('tags')", False, str(e))

section("TEST: explode - all source columns preserved in schema")
try:
    result = df_arrays.explode("tags")
    result_cols = result.columns
    ok("explode preserves all original columns",
       "id" in result_cols and "name" in result_cols and "tags" in result_cols,
       f"cols={result_cols}")
except Exception as e:
    ok("explode column preservation", False, str(e))

# ---------------------------------------------------------------------------
# unnest tests
# ---------------------------------------------------------------------------
section("TEST: unnest - flatten JSON object column into separate columns")
try:
    result = df_objects.unnest("metadata")
    result_cols = result.columns
    # Should expand 'metadata' into prefixed columns like metadata_city, metadata_age etc.
    new_cols = [c for c in result_cols if c.startswith("metadata_")]
    ok("unnest('metadata') creates prefixed sub-columns",
       len(new_cols) >= 2,
       f"new_cols={new_cols}")
except Exception as e:
    ok("unnest('metadata')", False, str(e))

section("TEST: unnest - row count preserved after unnest")
try:
    result = df_objects.unnest("metadata")
    ok("unnest preserves row count",
       result.shape[0] == 5,
       f"shape={result.shape}")
except Exception as e:
    ok("unnest row count", False, str(e))

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n{'=' * 60}")
print(f"  Gap 10 - Nested / Complex Data (SDK): RESULTS")
print(f"{'=' * 60}")
print(f"  RESULTS: {passed} passed, {failed} failed")
print(f"{'=' * 60}")

if failed > 0:
    print(f"\n  SOME TESTS FAILED ({failed})")
    sys.exit(1)
else:
    print(f"\n  ALL {passed} TESTS PASSED")
    sys.exit(0)
