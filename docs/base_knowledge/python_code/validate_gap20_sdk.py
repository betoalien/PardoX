#!/usr/bin/env python3
"""
validate_gap20_sdk.py - Gap 20: Time Travel / Dataset Versioning

Uses the public SDK exclusively (import pardox as pdx).
Validates the versioning API:
  - df.version_write(path, label, timestamp) -> writes versioned snapshot
  - pdx.version_list(path) -> list of version entries
  - pdx.version_read(path, label) -> DataFrame for a specific version
  - pdx.version_delete(path, label) -> removes a version

Test plan:
  Test 1 - df.version_write(path, "v1", 0) returns positive value
  Test 2 - pdx.version_list(path) returns a list with at least 1 entry
  Test 3 - "v1" label is present in version_list output
  Test 4 - pdx.version_read(path, "v1") returns DataFrame with correct rows
  Test 5 - pdx.version_delete(path, "v1") returns positive code
  Test 6 - After delete, "v1" is no longer in version_list
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


print("\n=== Gap 20: Time Travel / Dataset Versioning - SDK Validation ===\n")

# Load source data
try:
    df = pdx.read_csv(SALES)
    total_rows = df.shape[0]
    print(f"[PREP] Loaded sales.csv: {total_rows:,} rows\n")
except Exception as e:
    print(f"[FAIL] Could not load sales.csv: {e}")
    sys.exit(1)

# Create a temp directory as the version store
store_dir = tempfile.mkdtemp(prefix="pardox_gap20_sdk_")

# ── Test 1: df.version_write(path, "v1", 0) returns positive value ───────────
version_written = False
try:
    rc = df.version_write(store_dir, "v1", 0)
    ok("Test 1 - df.version_write(path, 'v1', 0) returns positive value",
       rc > 0,
       f"rc={rc}")
    version_written = rc > 0
except (NotImplementedError, RuntimeError, Exception) as e:
    ok("Test 1 - df.version_write(path, 'v1', 0) returns positive value",
       False, f"{type(e).__name__}: {str(e)[:100]}")

# ── Test 2: pdx.version_list(path) returns a list with at least 1 entry ──────
listing = []
if version_written:
    try:
        listing = pdx.version_list(store_dir)
        ok("Test 2 - pdx.version_list() returns a list with >= 1 entry",
           isinstance(listing, list) and len(listing) >= 1,
           f"entries={len(listing)}")
    except (NotImplementedError, Exception) as e:
        ok("Test 2 - pdx.version_list() returns a list with >= 1 entry",
           False, f"{type(e).__name__}: {str(e)[:100]}")
else:
    ok("Test 2 - pdx.version_list() [SKIP: write failed]", True, "[SKIP]")

# ── Test 3: "v1" label is present in version_list output ─────────────────────
if listing:
    # listing entries may be dicts with a "tag" field or plain strings
    def has_label(lst, label):
        for entry in lst:
            if isinstance(entry, dict):
                if entry.get("tag") == label or entry.get("label") == label:
                    return True
            elif isinstance(entry, str) and entry == label:
                return True
        return False

    ok("Test 3 - 'v1' label present in version_list output",
       has_label(listing, "v1"),
       f"listing={listing[:3]}")
else:
    ok("Test 3 - 'v1' label present in version_list [SKIP]", True, "[SKIP]")

# ── Test 4: pdx.version_read(path, "v1") returns DataFrame with correct rows ─
if version_written:
    try:
        df_v1 = pdx.version_read(store_dir, "v1")
        v1_rows = df_v1.shape[0]
        ok("Test 4 - pdx.version_read('v1') returns DataFrame with correct rows",
           isinstance(df_v1, pdx.DataFrame) and v1_rows == total_rows,
           f"read={v1_rows:,}  original={total_rows:,}")
    except (NotImplementedError, RuntimeError, Exception) as e:
        ok("Test 4 - pdx.version_read('v1') returns DataFrame with correct rows",
           False, f"{type(e).__name__}: {str(e)[:100]}")
else:
    ok("Test 4 - pdx.version_read('v1') [SKIP: write failed]", True, "[SKIP]")

# ── Test 5: pdx.version_delete(path, "v1") returns positive code ─────────────
delete_ok = False
if version_written:
    try:
        del_rc = pdx.version_delete(store_dir, "v1")
        ok("Test 5 - pdx.version_delete('v1') returns positive code",
           del_rc >= 0,
           f"rc={del_rc}")
        delete_ok = del_rc >= 0
    except (NotImplementedError, RuntimeError, Exception) as e:
        ok("Test 5 - pdx.version_delete('v1') returns positive code",
           False, f"{type(e).__name__}: {str(e)[:100]}")
else:
    ok("Test 5 - pdx.version_delete('v1') [SKIP: write failed]", True, "[SKIP]")

# ── Test 6: After delete, "v1" is no longer in version_list ──────────────────
if delete_ok:
    try:
        listing_after = pdx.version_list(store_dir)

        def has_label(lst, label):
            for entry in lst:
                if isinstance(entry, dict):
                    if entry.get("tag") == label or entry.get("label") == label:
                        return True
                elif isinstance(entry, str) and entry == label:
                    return True
            return False

        ok("Test 6 - After delete, 'v1' is no longer in version_list",
           not has_label(listing_after, "v1"),
           f"remaining={listing_after}")
    except (NotImplementedError, Exception) as e:
        ok("Test 6 - After delete, 'v1' is no longer in version_list",
           False, f"{type(e).__name__}: {str(e)[:100]}")
else:
    ok("Test 6 - After delete check [SKIP: delete failed or write failed]",
       True, "[SKIP]")

# Cleanup
shutil.rmtree(store_dir, ignore_errors=True)

print(f"\n{'='*60}")
print(f"  RESULTS: {passed} passed, {failed} failed")
print(f"{'='*60}")

if failed > 0:
    print(f"\n  {failed} test(s) FAILED")
    sys.exit(1)
else:
    print("  ALL TESTS PASSED - Gap 20 Time Travel SDK ✓")
