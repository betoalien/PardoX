#!/usr/bin/env python3
"""
Gap 11 - Spill to Disk / Out-of-Core (SDK version)
====================================================
Validates spill_to_disk, spill_from_disk, chunked_groupby, external_sort,
and memory_usage through the high-level Python SDK.
"""

import sys
import os
import tempfile
import pardox as pdx

SALES = "sales.csv"

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
# Load data
# ---------------------------------------------------------------------------
section("SETUP: Load sales.csv (100K rows)")
df = pdx.read_csv(SALES)
print(f"  Loaded: {df.shape[0]:,} rows, cols={df.columns}")
ok("sales.csv loaded", df.shape[0] == 100000, f"{df.shape[0]:,} rows")

# ---------------------------------------------------------------------------
# spill_to_disk / spill_from_disk
# ---------------------------------------------------------------------------
spill_path = os.path.join(tempfile.gettempdir(), "pardox_sdk_gap11_spill.pxs")

section("TEST 1 - spill_to_disk returns bytes > 0")
try:
    bytes_written = df.spill_to_disk(spill_path)
    ok("spill_to_disk returns positive bytes",
       isinstance(bytes_written, int) and bytes_written > 0,
       f"{bytes_written:,} bytes written to {spill_path}")
except Exception as e:
    ok("spill_to_disk", False, str(e))

section("TEST 2 - spill file exists on disk")
ok("spill file exists on disk",
   os.path.exists(spill_path) and os.path.getsize(spill_path) > 0,
   f"size={os.path.getsize(spill_path):,} bytes" if os.path.exists(spill_path) else "file missing")

section("TEST 3 - spill_from_disk restores DataFrame")
restored = None
try:
    restored = pdx.DataFrame.spill_from_disk(spill_path)
    ok("spill_from_disk returns DataFrame",
       isinstance(restored, pdx.DataFrame),
       f"type={type(restored).__name__}")
except Exception as e:
    ok("spill_from_disk", False, str(e))

section("TEST 4 - round-trip: row count preserved")
if restored is not None:
    ok("row count preserved after spill round-trip",
       restored.shape[0] == df.shape[0],
       f"original={df.shape[0]:,}, restored={restored.shape[0]:,}")
else:
    ok("row count preserved", False, "restored is None")

section("TEST 5 - round-trip: schema (columns) preserved")
if restored is not None:
    orig_cols = df.columns
    rest_cols = restored.columns
    ok("schema preserved after spill round-trip",
       orig_cols == rest_cols,
       f"original={orig_cols}, restored={rest_cols}")
else:
    ok("schema preserved", False, "restored is None")

# Cleanup spill file
if os.path.exists(spill_path):
    os.remove(spill_path)

# ---------------------------------------------------------------------------
# chunked_groupby
# ---------------------------------------------------------------------------
section("TEST 6 - chunked_groupby: sum by category (chunk_size=20000)")
try:
    chunked = df.chunked_groupby("category", {"price": "sum"}, 20000)
    ok("chunked_groupby returns DataFrame",
       isinstance(chunked, pdx.DataFrame) and chunked.shape[0] > 0,
       f"{chunked.shape[0]} groups")
except Exception as e:
    ok("chunked_groupby", False, str(e))

section("TEST 7 - chunked_groupby matches regular groupby group count")
try:
    chunked2 = df.chunked_groupby("category", {"price": "sum"}, 20000)
    inmem    = df.groupby("category", {"price": "sum"})
    ok("chunked_groupby group count matches in-memory groupby",
       chunked2.shape[0] == inmem.shape[0],
       f"chunked={chunked2.shape[0]}, inmem={inmem.shape[0]}")
except Exception as e:
    ok("chunked_groupby vs groupby", False, str(e))

# ---------------------------------------------------------------------------
# external_sort
# ---------------------------------------------------------------------------
section("TEST 8 - external_sort: ascending by price (chunk_size=20000)")
try:
    sorted_df = df.external_sort("price", ascending=True, chunk_size=20000)
    ok("external_sort returns DataFrame with same row count",
       isinstance(sorted_df, pdx.DataFrame) and sorted_df.shape[0] == df.shape[0],
       f"rows={sorted_df.shape[0]:,}")
except Exception as e:
    ok("external_sort", False, str(e))

# ---------------------------------------------------------------------------
# memory_usage
# ---------------------------------------------------------------------------
section("TEST 9 - memory_usage returns int > 0")
try:
    mem = pdx.DataFrame.memory_usage()
    ok("memory_usage returns positive integer",
       isinstance(mem, int) and mem > 0,
       f"{mem:,} bytes ({mem / (1024*1024):.1f} MB)")
except Exception as e:
    ok("memory_usage", False, str(e))

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n{'=' * 60}")
print(f"  Gap 11 - Spill to Disk / Out-of-Core (SDK): RESULTS")
print(f"{'=' * 60}")
print(f"  RESULTS: {passed} passed, {failed} failed")
print(f"{'=' * 60}")

if failed > 0:
    print(f"\n  SOME TESTS FAILED ({failed})")
    sys.exit(1)
else:
    print(f"\n  ALL {passed} TESTS PASSED")
    sys.exit(0)
