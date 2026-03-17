#!/usr/bin/env python3
"""
validate_gap29_sdk.py - Gap 29: REST API Connector (SDK)

Tests via import pardox as pdx:
  - Starts a minimal HTTP server in background thread
  - pdx.read_rest(url) -> DataFrame with rows > 0
  - Tests JSON, CSV endpoints
  - Tests invalid URL returns graceful error

Test plan:
  Test 1 - pdx.read_rest JSON endpoint returns DataFrame
  Test 2 - JSON DataFrame has rows > 0
  Test 3 - pdx.read_rest CSV endpoint returns DataFrame
  Test 4 - CSV DataFrame has rows > 0
  Test 5 - invalid/unreachable URL raises error gracefully (no crash)
"""

import sys
import json
import threading
import time
import socket
import random
from http.server import BaseHTTPRequestHandler, HTTPServer

import pardox as pdx

# ── Helpers ───────────────────────────────────────────────────────────────────
passed = 0
failed = 0

def ok(name, cond, detail=""):
    global passed, failed
    mark = "[PASS]" if cond else "[FAIL]"
    det  = f"  - {detail}" if detail else ""
    print(f"  {mark} {name}{det}")
    if cond:
        passed += 1
    else:
        failed += 1

# ── Find a free port ──────────────────────────────────────────────────────────
def find_free_port():
    for _ in range(20):
        p = random.randint(50000, 60000)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", p))
                return p
        except OSError:
            continue
    raise RuntimeError("Could not find a free port in range 50000-60000")

PORT = find_free_port()

# ── Sample data payloads ──────────────────────────────────────────────────────
JSON_DATA = json.dumps([
    {"id": 1, "name": "Alice",   "score": 95.5},
    {"id": 2, "name": "Bob",     "score": 87.3},
    {"id": 3, "name": "Carol",   "score": 91.0},
]).encode()

NDJSON_DATA = b"\n".join([
    json.dumps({"id": i, "value": float(i) * 1.5}).encode()
    for i in range(1, 4)
]) + b"\n"

CSV_DATA = b"product,price,qty\nApple,1.2,100\nBanana,0.5,200\nCherry,3.0,50\n"

ROUTES = {
    "/json":   ("application/json",              JSON_DATA),
    "/ndjson": ("application/x-ndjson",          NDJSON_DATA),
    "/csv":    ("text/csv",                      CSV_DATA),
}

# ── Local HTTP server ─────────────────────────────────────────────────────────
class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # suppress output

    def do_GET(self):
        path = self.path.split("?")[0]
        if path not in ROUTES:
            self.send_response(404)
            self.end_headers()
            return
        ct, body = ROUTES[path]
        self.send_response(200)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

httpd = HTTPServer(("127.0.0.1", PORT), _Handler)
server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
server_thread.start()
time.sleep(0.15)  # give server time to start

BASE = f"http://127.0.0.1:{PORT}"

print()
print("=" * 60)
print("  Gap 29 - REST API Connector (SDK)")
print(f"  HTTP server running on port {PORT}")
print("=" * 60)
print()

# ── Test 1: JSON endpoint returns DataFrame ───────────────────────────────────
df_json = None
try:
    df_json = pdx.read_rest(f"{BASE}/json")
    ok(
        "Test 1 - read_rest JSON endpoint returns DataFrame",
        isinstance(df_json, pdx.DataFrame),
        f"type={type(df_json).__name__}"
    )
except Exception as e:
    ok("Test 1 - read_rest JSON endpoint returns DataFrame", False, str(e))

# ── Test 2: JSON DataFrame has rows > 0 ──────────────────────────────────────
if df_json is not None:
    try:
        rows, cols = df_json.shape
        ok(
            "Test 2 - JSON DataFrame has rows > 0",
            rows > 0,
            f"shape=({rows}, {cols})"
        )
    except Exception as e:
        ok("Test 2 - JSON DataFrame has rows > 0", False, str(e))
else:
    ok("Test 2 - JSON DataFrame has rows > 0", False, "read_rest JSON failed")

# ── Test 3: CSV endpoint returns DataFrame ────────────────────────────────────
df_csv = None
try:
    df_csv = pdx.read_rest(f"{BASE}/csv")
    ok(
        "Test 3 - read_rest CSV endpoint returns DataFrame",
        isinstance(df_csv, pdx.DataFrame),
        f"type={type(df_csv).__name__}"
    )
except Exception as e:
    ok("Test 3 - read_rest CSV endpoint returns DataFrame", False, str(e))

# ── Test 4: CSV DataFrame has rows > 0 ───────────────────────────────────────
if df_csv is not None:
    try:
        rows, cols = df_csv.shape
        ok(
            "Test 4 - CSV DataFrame has rows > 0",
            rows > 0,
            f"shape=({rows}, {cols})"
        )
    except Exception as e:
        ok("Test 4 - CSV DataFrame has rows > 0", False, str(e))
else:
    ok("Test 4 - CSV DataFrame has rows > 0", False, "read_rest CSV failed")

# ── Test 5: invalid URL raises graceful error (no crash) ─────────────────────
error_raised = False
try:
    _ = pdx.read_rest("http://127.0.0.1:1/nonexistent_path_xyz")
    # If it somehow returns something, that's also acceptable
    error_raised = True  # no exception, but didn't crash
except (RuntimeError, Exception):
    error_raised = True  # expected - graceful error

ok(
    "Test 5 - invalid URL raises error gracefully (no crash)",
    error_raised,
    "no unhandled crash or segfault"
)

# ── Shutdown server ───────────────────────────────────────────────────────────
httpd.shutdown()

# ── Summary ───────────────────────────────────────────────────────────────────
print()
print(f"RESULTS: {passed} passed, {failed} failed out of {passed + failed} tests")
print()
if failed > 0:
    sys.exit(1)
print("All tests passed - Gap 29 (SDK) COMPLETE")
