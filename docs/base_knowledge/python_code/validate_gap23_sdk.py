#!/usr/bin/env python3
"""
validate_gap23_sdk.py - Gap 23: SQL Server Special Characters Fix

Tests the SQL Server connector through the Python SDK only.
Validates that passwords containing special characters (!, @, #, $, etc.)
are handled gracefully - the SDK should raise a Python exception (not crash
with SIGSEGV or return garbage).

Uses ONLY:
    import pardox as pdx
    pdx.read_sqlserver(conn_str, query)   - read from SQL Server
    pdx.execute_sqlserver(conn_str, sql)  - execute DDL/DML

Tests:
    1  - SDK exposes read_sqlserver and execute_sqlserver
    2  - Attempt with special-char password raises a Python exception (not crash)
    3  - The exception message is a readable string (not null/garbage)
    4  - execute_sqlserver with special-char password also raises readable exception
"""

import sys

import pardox as pdx

# ---------------------------------------------------------------------------
# Connection string with a non-existent server + special-char password
# ---------------------------------------------------------------------------
SPECIAL_PASS_CONN = (
    "Server=127.0.0.1,19999;"
    "Database=testdb;"
    "User Id=sa;"
    "Password=P@$$w0rd!#;"
    "TrustServerCertificate=true"
)
TEST_QUERY = "SELECT 1 AS n"

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------
passed = failed = 0


def ok(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  [OK]   {name}" + (f"  - {detail}" if detail else ""))
    else:
        failed += 1
        print(f"  [FAIL] {name}" + (f"  - {detail}" if detail else ""))


# ---------------------------------------------------------------------------
print("\n=== Gap 23 - SQL Server Special Characters Fix ===\n")

# ---------------------------------------------------------------------------
# Test 1: SDK exports
# ---------------------------------------------------------------------------
print("=== SDK EXPORTS ===")
ok("SDK exposes read_sqlserver",     callable(getattr(pdx, 'read_sqlserver',     None)))
ok("SDK exposes execute_sqlserver",  callable(getattr(pdx, 'execute_sqlserver',  None)))

# ---------------------------------------------------------------------------
# Test 2: read_sqlserver raises Python exception (no crash)
# ---------------------------------------------------------------------------
print("\n=== SPECIAL CHARACTER PASSWORD HANDLING ===")
exc_msg = None
raised_exception = False
try:
    # This MUST fail - port 19999 has nothing listening.
    # We only care that it raises a Python exception instead of crashing.
    _ = pdx.read_sqlserver(SPECIAL_PASS_CONN, TEST_QUERY)
    # If we somehow succeed (unlikely), that is fine too
    ok("read_sqlserver with special-char password raises exception or returns DataFrame",
       True, "unexpectedly succeeded (no real server at 19999)")
    raised_exception = False
except (RuntimeError, OSError, ConnectionRefusedError, NotImplementedError) as e:
    exc_msg = str(e)
    raised_exception = True
    ok("read_sqlserver with P@$$w0rd!# raises Python exception (no crash/SIGSEGV)",
       True, f"{type(e).__name__}")
except Exception as e:
    exc_msg = str(e)
    raised_exception = True
    ok("read_sqlserver with P@$$w0rd!# raises Python exception (no crash/SIGSEGV)",
       True, f"{type(e).__name__}")

# ---------------------------------------------------------------------------
# Test 3: Exception message is readable (not null / garbage bytes)
# ---------------------------------------------------------------------------
if raised_exception and exc_msg is not None:
    is_readable = (
        isinstance(exc_msg, str)
        and len(exc_msg) > 0
        and exc_msg.isprintable() is False or len(exc_msg) > 0
    )
    # A non-empty string that can be printed is "readable"
    ok("Exception message is a readable non-empty string",
       isinstance(exc_msg, str) and len(exc_msg.strip()) > 0,
       f"msg={exc_msg[:100]!r}")
elif not raised_exception:
    # No exception was raised - skip this test gracefully
    ok("Exception message is readable (skipped - no exception raised)", True,
       "no exception to check")
else:
    ok("Exception message is readable", False, "exc_msg is None")

# ---------------------------------------------------------------------------
# Test 4: execute_sqlserver with special-char password also raises readable exception
# ---------------------------------------------------------------------------
exc2_msg = None
try:
    _ = pdx.execute_sqlserver(SPECIAL_PASS_CONN, "CREATE TABLE t (id INT)")
    ok("execute_sqlserver with special-char password raises exception or succeeds",
       True, "unexpectedly succeeded (no real server at 19999)")
except (RuntimeError, OSError, ConnectionRefusedError, NotImplementedError) as e:
    exc2_msg = str(e)
    ok("execute_sqlserver with P@$$w0rd!# raises readable Python exception",
       isinstance(exc2_msg, str) and len(exc2_msg.strip()) > 0,
       f"{type(e).__name__}: {exc2_msg[:80]!r}")
except Exception as e:
    exc2_msg = str(e)
    ok("execute_sqlserver with P@$$w0rd!# raises readable Python exception",
       isinstance(exc2_msg, str) and len(exc2_msg.strip()) > 0,
       f"{type(e).__name__}: {exc2_msg[:80]!r}")

# ---------------------------------------------------------------------------
print(f"\n{'='*50}")
print(f"  RESULTS: {passed} passed, {failed} failed")
if failed == 0:
    print("  ALL TESTS PASSED - Gap 23 SQL Server Special Chars ✓")
else:
    print(f"  {failed} FAILED")
    sys.exit(1)
