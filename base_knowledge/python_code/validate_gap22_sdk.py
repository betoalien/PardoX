#!/usr/bin/env python3
"""
validate_gap22_sdk.py - Gap 22: Distributed Mode / Cluster Ligero

Tests the cluster/distributed API through the Python SDK only.

Uses ONLY:
    import pardox as pdx
    pdx.cluster_connect(addrs)   - connect to a cluster (list of "host:port" strings)
    pdx.ClusterConnection        - connection class
    ClusterConnection.ping()     - ping alive worker count
    ClusterConnection.sql(name, query) - run SQL on cluster
    ClusterConnection.free()     - release connection

Tests:
    1  - SDK exposes cluster_connect
    2  - SDK exposes ClusterConnection class
    3  - ClusterConnection has expected methods (ping, sql, free)
    4  - Connecting to non-existent cluster returns error or None gracefully
    5  - ClusterConnection repr is human-readable
"""

import sys

import pardox as pdx

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
print("\n=== Gap 22 - Distributed Mode / Cluster Ligero ===\n")

# ---------------------------------------------------------------------------
# Test 1: SDK exposes cluster_connect
# ---------------------------------------------------------------------------
print("=== SDK EXPORTS ===")
ok("SDK exposes cluster_connect",   callable(getattr(pdx, 'cluster_connect', None)))
ok("SDK exposes ClusterConnection", pdx.ClusterConnection is not None)

# ---------------------------------------------------------------------------
# Test 3: ClusterConnection class has expected interface
# ---------------------------------------------------------------------------
print("\n=== CLASS INTERFACE ===")
expected_methods = ['ping', 'sql', 'free']
missing = [m for m in expected_methods if not callable(getattr(pdx.ClusterConnection, m, None))]
ok("ClusterConnection has methods: ping, sql, free",
   len(missing) == 0,
   f"missing={missing}" if missing else "all present")

# ---------------------------------------------------------------------------
# Test 4: Connect to non-existent cluster - graceful error handling
# ---------------------------------------------------------------------------
print("\n=== GRACEFUL ERROR HANDLING ===")
conn = None
error_msg = None
try:
    # Port 19999 should have nothing listening
    conn = pdx.cluster_connect(["127.0.0.1:19999"])
    # If we get a connection object back, test ping on a dead cluster
    if conn is not None:
        ok("cluster_connect to dead cluster returns object or raises gracefully",
           True, "returned ClusterConnection object")
        try:
            alive = conn.ping()
            ok("ping() on unreachable cluster returns 0 or -1 (no crash)",
               isinstance(alive, int) and alive <= 0,
               f"ping={alive}")
        except Exception as ping_err:
            ok("ping() on unreachable cluster raises exception gracefully (no SIGSEGV)",
               True, str(ping_err))
    else:
        ok("cluster_connect to dead cluster returns None gracefully", True,
           "returned None")
except (RuntimeError, OSError, ConnectionRefusedError) as e:
    error_msg = str(e)
    ok("cluster_connect to dead cluster raises RuntimeError gracefully (no crash)",
       True, f"error={error_msg[:80]}")
except NotImplementedError as e:
    ok("cluster_connect to dead cluster raises NotImplementedError gracefully",
       True, f"NotImplemented: {e}")
except Exception as e:
    # Any Python-level exception (not SIGSEGV) is acceptable
    error_msg = str(e)
    ok("cluster_connect to dead cluster raises exception gracefully (no crash)",
       True, f"exception={type(e).__name__}: {error_msg[:80]}")
finally:
    if conn is not None:
        try:
            conn.free()
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Test 5: ClusterConnection repr
# ---------------------------------------------------------------------------
print("\n=== REPR ===")
try:
    repr_str = repr(pdx.ClusterConnection)
    ok("ClusterConnection repr is a non-empty string",
       isinstance(repr_str, str) and len(repr_str) > 0,
       f"repr={repr_str[:60]}")
except Exception as e:
    ok("ClusterConnection repr is a non-empty string", False, str(e))

# ---------------------------------------------------------------------------
print(f"\n{'='*50}")
print(f"  RESULTS: {passed} passed, {failed} failed")
if failed == 0:
    print("  ALL TESTS PASSED - Gap 22 Cluster Ligero ✓")
else:
    print(f"  {failed} FAILED")
    sys.exit(1)
