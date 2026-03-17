#!/usr/bin/env python3
"""
Gap 6 - pardoX Server (PostgreSQL Wire Protocol)
=================================================
Validates the pardoX PostgreSQL wire-protocol server through the Python SDK.

Uses ONLY:
    import pardox as pdx
    pdx.PardoXPostgresServer  - server class
    pdx.Registry              - table registry
    pdx.PardoxServerEngine    - pardox_cpu engine wrapper

Data:
    ventas_consolidado.prdx - columns: transaction_id, client_id, date_time,
                              entity, category, client_segment, amount, tax_rate

Tests:
    1  - SDK exposes PardoXPostgresServer, Registry, PardoxServerEngine
    2  - Server starts on test port
    3  - Duplicate start() returns False
    4  - TCP connect + SSL negotiation returns 'N'
    5  - Auth with correct credentials succeeds
    6  - Auth with wrong password returns ErrorResponse
    7  - SELECT version() returns pardoX version string
    8  - SET search_path handled without error
    9  - SELECT * FROM ventas LIMIT 5 returns 5 rows
    10 - SELECT COUNT(*) returns scalar > 0
    11 - Schema has all 8 expected columns
    12 - amount column values parse as float
    13 - GROUP BY entity returns > 1 groups
    14 - WHERE amount > 1000 LIMIT 5 returns only amounts > 1000
    15 - ORDER BY amount DESC LIMIT 5 returns descending order
    16 - SHOW server_version returns '14.0'
    17 - Unknown table returns empty result (no error)
    18 - Server stops cleanly
"""

import sys

import pardox as pdx

import os
import socket
import struct
import tempfile
import time

# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------
PRDX_PATH = 'ventas_consolidado.prdx'
TEST_PORT  = 15433
TEST_USER  = 'pardox_test'
TEST_PASS  = 'test_secret_99'

EXPECTED_COLS = {
    'transaction_id', 'client_id', 'date_time',
    'entity', 'category', 'client_segment', 'amount', 'tax_rate'
}

# ---------------------------------------------------------------------------
# Helpers
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


# ---------------------------------------------------------------------------
# PostgreSQL wire-protocol helpers (no psycopg2 required)
# ---------------------------------------------------------------------------

def _recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def _read_message(sock):
    hdr = _recv_exact(sock, 5)
    if not hdr:
        return None
    msg_type = chr(hdr[0])
    msg_len  = struct.unpack('>I', hdr[1:5])[0]
    body_len = msg_len - 4
    body     = _recv_exact(sock, body_len) if body_len > 0 else b''
    return msg_type, (body or b'')


def _drain_until_ready(sock):
    msgs = []
    while True:
        m = _read_message(sock)
        if m is None:
            break
        msgs.append(m)
        if m[0] == 'Z':
            break
    return msgs


def _build_startup(user):
    params = (b'user\x00' + user.encode() + b'\x00'
              + b'database\x00postgres\x00'
              + b'application_name\x00pardox_test\x00'
              + b'\x00')
    length = 4 + 4 + len(params)
    return struct.pack('>II', length, 196608) + params


def _do_auth(sock, user, password):
    sock.sendall(struct.pack('>II', 8, 80877103))  # SSLRequest
    if sock.recv(1) != b'N':
        return False
    sock.sendall(_build_startup(user))
    while True:
        m = _read_message(sock)
        if not m:
            return False
        t, body = m
        if t == 'R':
            auth_type = struct.unpack('>I', body[:4])[0]
            if auth_type == 3:   # CleartextPassword
                pw_body = password.encode() + b'\x00'
                sock.sendall(b'p' + struct.pack('>I', 4 + len(pw_body)) + pw_body)
            elif auth_type == 0:
                break
            else:
                return False
        elif t == 'E':
            return False
    while True:
        m = _read_message(sock)
        if not m:
            return False
        if m[0] == 'Z':
            return True
        if m[0] == 'E':
            return False


def _simple_query(sock, sql):
    body = sql.encode() + b'\x00'
    sock.sendall(b'Q' + struct.pack('>I', 4 + len(body)) + body)
    return _drain_until_ready(sock)


def _parse_row_description(body):
    n_cols = struct.unpack('>H', body[:2])[0]
    pos, names = 2, []
    for _ in range(n_cols):
        end = body.index(b'\x00', pos)
        names.append(body[pos:end].decode('utf-8'))
        pos = end + 1 + 18
    return names


def _parse_data_rows(messages):
    rows = []
    for t, body in messages:
        if t != 'D':
            continue
        n_cols = struct.unpack('>H', body[:2])[0]
        pos, row = 2, []
        for _ in range(n_cols):
            col_len = struct.unpack('>i', body[pos:pos+4])[0]
            pos += 4
            if col_len == -1:
                row.append(None)
            else:
                row.append(body[pos:pos+col_len])
                pos += col_len
        rows.append(row)
    return rows


def _connect():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(60)
    s.connect(('127.0.0.1', TEST_PORT))
    return s


# ---------------------------------------------------------------------------
print("\n=== Gap 6 - pardoX Server (PostgreSQL Wire Protocol) ===\n")

# ---------------------------------------------------------------------------
# Test 1: SDK exposes server components
# ---------------------------------------------------------------------------
print("=== SDK EXPORTS ===")
ok("SDK exposes PardoXPostgresServer", pdx.PardoXPostgresServer is not None)
ok("SDK exposes Registry",             pdx.Registry is not None)
ok("SDK exposes PardoxServerEngine",   pdx.PardoxServerEngine is not None)

if pdx.PardoXPostgresServer is None:
    print("[FATAL] Server components not exported by SDK. Cannot continue.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Setup: create engine, registry, server via SDK
# ---------------------------------------------------------------------------
config_dir = tempfile.mkdtemp(prefix='pardox_gap6_test_')
engine   = pdx.PardoxServerEngine()
registry = pdx.Registry(config_dir)
server   = pdx.PardoXPostgresServer(
    engine, registry,
    port=TEST_PORT, username=TEST_USER, password=TEST_PASS,
)

reg_err = registry.add('ventas', PRDX_PATH)
if reg_err:
    print(f"[FATAL] Registry.add failed: {reg_err}")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Test 2: Server starts
# ---------------------------------------------------------------------------
print("\n=== SERVER START ===")
result = server.start()
ok("Server starts successfully", result is True)
time.sleep(0.15)

# Test 3: Duplicate start
ok("Duplicate start() returns False", server.start() is False)

# ---------------------------------------------------------------------------
# Test 4: TCP + SSL negotiation
# ---------------------------------------------------------------------------
print("\n=== CONNECTION ===")
try:
    sock4 = _connect()
    sock4.sendall(struct.pack('>II', 8, 80877103))
    resp = sock4.recv(1)
    ok("SSL negotiation returns 'N'", resp == b'N')
    sock4.close()
except Exception as e:
    ok("SSL negotiation returns 'N'", False, str(e))

# ---------------------------------------------------------------------------
# Test 5: Auth success
# ---------------------------------------------------------------------------
print("\n=== AUTHENTICATION ===")
sock = None
auth_ok = False
try:
    sock = _connect()
    auth_ok = _do_auth(sock, TEST_USER, TEST_PASS)
    ok("Auth with correct credentials succeeds", auth_ok)
except Exception as e:
    ok("Auth with correct credentials succeeds", False, str(e))

# Test 6: Auth failure
try:
    sock6 = _connect()
    sock6.sendall(struct.pack('>II', 8, 80877103))
    sock6.recv(1)
    sock6.sendall(_build_startup(TEST_USER))
    m6 = _read_message(sock6)
    if m6 and m6[0] == 'R':
        pw = 'wrong_password_999'.encode() + b'\x00'
        sock6.sendall(b'p' + struct.pack('>I', 4 + len(pw)) + pw)
        m6b = _read_message(sock6)
        ok("Auth with wrong password returns ErrorResponse",
           m6b is not None and m6b[0] == 'E')
    else:
        ok("Auth with wrong password returns ErrorResponse", False, "no auth request")
    sock6.close()
except Exception:
    ok("Auth with wrong password returns ErrorResponse", True, "connection closed")

# ---------------------------------------------------------------------------
# Test 7: SELECT version()
# ---------------------------------------------------------------------------
print("\n=== SQL QUERIES ===")
if auth_ok and sock:
    try:
        msgs = _simple_query(sock, "SELECT version()")
        rows = _parse_data_rows(msgs)
        ok("SELECT version() returns pardoX string",
           len(rows) == 1 and rows[0][0] and b'pardoX' in rows[0][0],
           f"val={rows[0][0][:50] if rows else None}")
    except Exception as e:
        ok("SELECT version() returns pardoX string", False, str(e))

# Test 8: SET
if auth_ok and sock:
    try:
        msgs = _simple_query(sock, "SET search_path = 'public'")
        ok("SET handled without error",
           any(t == 'Z' for t, _ in msgs))
    except Exception as e:
        ok("SET handled without error", False, str(e))

# Test 9: SELECT * LIMIT 5
if auth_ok and sock:
    try:
        msgs = _simple_query(sock, "SELECT * FROM public.ventas LIMIT 5")
        rows = _parse_data_rows(msgs)
        ok("SELECT * LIMIT 5 returns 5 rows", len(rows) == 5, f"rows={len(rows)}")
    except Exception as e:
        ok("SELECT * LIMIT 5 returns 5 rows", False, str(e))

# Test 10: COUNT(*)
if auth_ok and sock:
    try:
        msgs = _simple_query(sock, "SELECT COUNT(*) AS n FROM public.ventas")
        rows = _parse_data_rows(msgs)
        n = int(rows[0][0]) if rows and rows[0][0] else 0
        ok("SELECT COUNT(*) returns scalar > 0", len(rows) == 1 and n > 0, f"n={n}")
    except Exception as e:
        ok("SELECT COUNT(*) returns scalar > 0", False, str(e))

# Test 11: Schema columns
if auth_ok and sock:
    try:
        msgs = _simple_query(sock, "SELECT * FROM public.ventas LIMIT 1")
        rd_body = next((body for t, body in msgs if t == 'T'), None)
        col_names = set(_parse_row_description(rd_body)) if rd_body else set()
        ok("Schema has all 8 expected columns",
           EXPECTED_COLS == col_names, f"got={col_names}")
    except Exception as e:
        ok("Schema has all 8 expected columns", False, str(e))

# Test 12: amount as float
if auth_ok and sock:
    try:
        msgs = _simple_query(sock, "SELECT amount FROM public.ventas LIMIT 10")
        rows = _parse_data_rows(msgs)
        rd   = next((body for t, body in msgs if t == 'T'), None)
        cols = _parse_row_description(rd) if rd else []
        idx  = cols.index('amount') if 'amount' in cols else 0
        amounts = [float(r[idx]) for r in rows if r[idx]]
        ok("amount column values parse as float",
           len(amounts) > 0, f"first={amounts[0]:.2f}" if amounts else "empty")
    except Exception as e:
        ok("amount column values parse as float", False, str(e))

# Test 13: GROUP BY
if auth_ok and sock:
    try:
        msgs = _simple_query(sock,
            "SELECT entity, SUM(amount) AS total FROM public.ventas GROUP BY entity")
        rows = _parse_data_rows(msgs)
        ok("GROUP BY entity returns > 1 groups", len(rows) > 1, f"groups={len(rows)}")
    except Exception as e:
        ok("GROUP BY entity returns > 1 groups", False, str(e))

# Test 14: WHERE filter
if auth_ok and sock:
    try:
        msgs = _simple_query(sock,
            "SELECT amount FROM public.ventas WHERE amount > 1000 LIMIT 5")
        rows = _parse_data_rows(msgs)
        rd   = next((body for t, body in msgs if t == 'T'), None)
        cols = _parse_row_description(rd) if rd else []
        idx  = cols.index('amount') if 'amount' in cols else 0
        amounts = [float(r[idx]) for r in rows if r and r[idx]]
        ok("WHERE amount > 1000 - all values > 1000",
           len(amounts) > 0 and all(a > 1000 for a in amounts),
           f"count={len(amounts)}")
    except Exception as e:
        ok("WHERE amount > 1000 - all values > 1000", False, str(e))

# Test 15: ORDER BY DESC
if auth_ok and sock:
    try:
        msgs = _simple_query(sock,
            "SELECT amount FROM public.ventas ORDER BY amount DESC LIMIT 5")
        rows = _parse_data_rows(msgs)
        rd   = next((body for t, body in msgs if t == 'T'), None)
        cols = _parse_row_description(rd) if rd else []
        idx  = cols.index('amount') if 'amount' in cols else 0
        amounts = [float(r[idx]) for r in rows if r and r[idx]]
        desc_ok = all(amounts[i] >= amounts[i+1] for i in range(len(amounts)-1))
        ok("ORDER BY amount DESC LIMIT 5 - values descending",
           len(amounts) == 5 and desc_ok,
           f"amounts={[round(a,2) for a in amounts]}")
    except Exception as e:
        ok("ORDER BY amount DESC LIMIT 5 - values descending", False, str(e))

# Test 16: SHOW server_version
if auth_ok and sock:
    try:
        msgs = _simple_query(sock, "SHOW server_version")
        rows = _parse_data_rows(msgs)
        val  = rows[0][0].decode() if rows and rows[0][0] else ''
        ok("SHOW server_version returns '14.0'", val == '14.0', f"got='{val}'")
    except Exception as e:
        ok("SHOW server_version returns '14.0'", False, str(e))

# Test 17: Unknown table
if auth_ok and sock:
    try:
        msgs = _simple_query(sock, "SELECT * FROM public.nonexistent_table LIMIT 5")
        rows = _parse_data_rows(msgs)
        has_rfq = any(t == 'Z' for t, _ in msgs)
        ok("Unknown table returns empty result (no error)",
           has_rfq and len(rows) == 0, f"rows={len(rows)}")
    except Exception as e:
        ok("Unknown table returns empty result (no error)", False, str(e))

# ---------------------------------------------------------------------------
# Test 18: Stop server
# ---------------------------------------------------------------------------
print("\n=== SHUTDOWN ===")
if sock:
    try:
        sock.sendall(b'X\x00\x00\x00\x04')
    except Exception:
        pass
    sock.close()

server.stop()
time.sleep(0.2)
ok("Server stops cleanly", not server.is_running())

# ---------------------------------------------------------------------------
print(f"\n{'='*50}")
print(f"  RESULTS: {passed} passed, {failed} failed")
if failed == 0:
    print("  ALL TESTS PASSED - Gap 6 PG Wire Server ✓")
else:
    print(f"  {failed} FAILED")
    sys.exit(1)
