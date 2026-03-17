#!/usr/bin/env python3
"""
validate_gap24_sdk.py - Gap 24: INNER JOIN via pardoX PG Server

Tests INNER JOIN routing added to pg_server.py through the Python SDK only.
Loads sales.csv + customers.csv, saves as .prdx, registers both in the
pardoX PostgreSQL wire-protocol server, and validates JOIN queries via the
raw wire protocol.

Uses ONLY:
    import pardox as pdx
    pdx.PardoXPostgresServer  - server class
    pdx.Registry              - table registry
    pdx.PardoxServerEngine    - engine wrapper
    pdx.read_csv(path)        - load CSV
    df.to_prdx(path)          - save as .prdx

Tests:
     1 - SDK exposes PardoXPostgresServer, Registry, PardoxServerEngine
     2 - Server starts on test port
     3 - TCP connect + auth succeeds
     4 - INNER JOIN returns rows (basic)
     5 - JOIN row count > 0 and <= LIMIT
     6 - JOIN result has columns from both tables
     7 - JOIN with WHERE filter returns rows
     8 - JOIN with LIMIT 7 returns exactly 7 rows (or less if fewer matches)
     9 - price column from JOIN is parseable as float
    10 - id_customer column present in JOIN result
    11 - Second JOIN query succeeds (manager cache reuse, no crash)
    12 - COUNT(*) over JOIN result returns scalar > 0
    13 - Server stops cleanly
"""

import sys

import pardox as pdx

import os
import shutil
import socket
import struct
import tempfile
import time

# ---------------------------------------------------------------------------
# Data paths
# ---------------------------------------------------------------------------
SALES_CSV = "sales.csv"
CUST_CSV  = "customers.csv"

TEST_PORT = 15434
TEST_USER = "pardox_user"
TEST_PASS = "test_secret_24"

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
              + b'application_name\x00pardox_gap24\x00'
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
                row.append(body[pos:pos+col_len].decode('utf-8', errors='replace'))
                pos += col_len
        rows.append(row)
    return rows


def _connect():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(30)
    s.connect(('127.0.0.1', TEST_PORT))
    return s


# ---------------------------------------------------------------------------
print("\n=== Gap 24 - INNER JOIN via pardoX PG Server ===\n")

# ---------------------------------------------------------------------------
# Test 1: SDK exports
# ---------------------------------------------------------------------------
print("=== SDK EXPORTS ===")
ok("SDK exposes PardoXPostgresServer", pdx.PardoXPostgresServer is not None)
ok("SDK exposes Registry",             pdx.Registry is not None)
ok("SDK exposes PardoxServerEngine",   pdx.PardoxServerEngine is not None)

if pdx.PardoXPostgresServer is None:
    print("[FATAL] Server components not exported by SDK. Cannot continue.")
    print(f"\n{'='*50}")
    print(f"  RESULTS: {passed} passed, {failed} failed")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Setup: load CSVs, save as .prdx, create server
# ---------------------------------------------------------------------------
print("\n=== SETUP ===")
tmp_dir = tempfile.mkdtemp(prefix='pardox_gap24_sdk_')
prdx_sales = os.path.join(tmp_dir, 'sales.prdx')
prdx_cust  = os.path.join(tmp_dir, 'customers.prdx')

df_sales = df_cust = None
setup_ok = False
try:
    df_sales = pdx.read_csv(SALES_CSV)
    df_cust  = pdx.read_csv(CUST_CSV)
    df_sales.to_prdx(prdx_sales)
    df_cust.to_prdx(prdx_cust)
    sales_rows, _ = df_sales.shape
    cust_rows, _  = df_cust.shape
    print(f"  [INFO] sales.prdx: {sales_rows:,} rows  |  customers.prdx: {cust_rows:,} rows")
    setup_ok = True
except Exception as e:
    print(f"  [ERROR] Setup failed: {e}")
    shutil.rmtree(tmp_dir, ignore_errors=True)
    print(f"\n{'='*50}")
    print(f"  RESULTS: {passed} passed, {failed} failed")
    sys.exit(1)

# Build server
engine   = pdx.PardoxServerEngine()
registry = pdx.Registry(tmp_dir)
reg_err  = registry.add('sales',     prdx_sales)
reg_err2 = registry.add('customers', prdx_cust)
if reg_err or reg_err2:
    print(f"  [ERROR] Registry.add failed: {reg_err or reg_err2}")
    shutil.rmtree(tmp_dir, ignore_errors=True)
    print(f"\n{'='*50}")
    print(f"  RESULTS: {passed} passed, {failed} failed")
    sys.exit(1)

server = pdx.PardoXPostgresServer(
    engine, registry,
    port=TEST_PORT, username=TEST_USER, password=TEST_PASS,
)

# ---------------------------------------------------------------------------
# Test 2: Server starts
# ---------------------------------------------------------------------------
print("\n=== SERVER START ===")
started = server.start()
ok("Server starts on port 15434", started is True, f"result={started}")
time.sleep(0.25)

# ---------------------------------------------------------------------------
# Test 3: TCP connect + auth
# ---------------------------------------------------------------------------
print("\n=== CONNECTION ===")
sock = None
auth_ok = False
try:
    sock = _connect()
    auth_ok = _do_auth(sock, TEST_USER, TEST_PASS)
    ok("TCP connect + auth succeeds", auth_ok)
except Exception as e:
    ok("TCP connect + auth succeeds", False, str(e))

if not auth_ok or sock is None:
    print("  [FATAL] Cannot connect to server - skipping query tests.")
    server.stop()
    shutil.rmtree(tmp_dir, ignore_errors=True)
    print(f"\n{'='*50}")
    print(f"  RESULTS: {passed} passed, {failed} failed")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Test 4: INNER JOIN returns rows
# ---------------------------------------------------------------------------
print("\n=== JOIN QUERIES ===")
cols_join = []
rows_join = []
try:
    msgs = _simple_query(sock,
        "SELECT * FROM sales INNER JOIN customers "
        "ON sales.id_customer = customers.id_customer LIMIT 20")
    rd_body   = next((body for t, body in msgs if t == 'T'), None)
    cols_join = _parse_row_description(rd_body) if rd_body else []
    rows_join = _parse_data_rows(msgs)
    ok("INNER JOIN returns rows",
       len(rows_join) > 0,
       f"{len(rows_join)} rows, {len(cols_join)} cols")
except Exception as e:
    ok("INNER JOIN returns rows", False, str(e))

# ---------------------------------------------------------------------------
# Test 5: JOIN row count > 0 and <= LIMIT
# ---------------------------------------------------------------------------
try:
    msgs2 = _simple_query(sock,
        "SELECT * FROM sales INNER JOIN customers "
        "ON sales.id_customer = customers.id_customer LIMIT 500")
    rows2 = _parse_data_rows(msgs2)
    ok("JOIN row count > 0 and <= LIMIT 500",
       0 < len(rows2) <= 500, f"rows={len(rows2)}")
except Exception as e:
    ok("JOIN row count > 0 and <= LIMIT 500", False, str(e))

# ---------------------------------------------------------------------------
# Test 6: JOIN result has columns from both tables
# ---------------------------------------------------------------------------
col_set = set(cols_join)
has_sales_col = bool({'id_transaction', 'price', 'category'} & col_set)
has_cust_col  = bool({'first_name', 'last_name', 'state'} & col_set)
ok("JOIN result has columns from both tables",
   has_sales_col and has_cust_col,
   f"cols={sorted(col_set)[:8]}...")

# ---------------------------------------------------------------------------
# Test 7: JOIN with WHERE filter
# ---------------------------------------------------------------------------
try:
    msgs3 = _simple_query(sock,
        "SELECT * FROM sales INNER JOIN customers "
        "ON sales.id_customer = customers.id_customer "
        "WHERE price > 500 LIMIT 50")
    rows3 = _parse_data_rows(msgs3)
    ok("JOIN with WHERE price > 500 returns rows",
       len(rows3) > 0, f"rows={len(rows3)}")
except Exception as e:
    ok("JOIN with WHERE price > 500 returns rows", False, str(e))

# ---------------------------------------------------------------------------
# Test 8: JOIN with LIMIT 7
# ---------------------------------------------------------------------------
try:
    msgs4 = _simple_query(sock,
        "SELECT * FROM sales INNER JOIN customers "
        "ON sales.id_customer = customers.id_customer LIMIT 7")
    rows4 = _parse_data_rows(msgs4)
    ok("JOIN with LIMIT 7 returns <= 7 rows",
       0 < len(rows4) <= 7, f"rows={len(rows4)}")
except Exception as e:
    ok("JOIN with LIMIT 7 returns <= 7 rows", False, str(e))

# ---------------------------------------------------------------------------
# Test 9: price column parseable as float
# ---------------------------------------------------------------------------
if rows_join and 'price' in cols_join:
    pi = cols_join.index('price')
    try:
        sample = [float(r[pi]) for r in rows_join if r[pi] is not None]
        ok("price column from JOIN parses as float",
           len(sample) > 0,
           f"sample={[round(v, 2) for v in sample[:3]]}")
    except (ValueError, TypeError) as e:
        ok("price column from JOIN parses as float", False, str(e))
else:
    ok("price column from JOIN parses as float",
       False, f"'price' not in cols={cols_join}")

# ---------------------------------------------------------------------------
# Test 10: id_customer present in JOIN result
# ---------------------------------------------------------------------------
ok("id_customer column present in JOIN result",
   'id_customer' in cols_join, f"cols={cols_join}")

# ---------------------------------------------------------------------------
# Test 11: Second JOIN query (cache reuse, no crash)
# ---------------------------------------------------------------------------
try:
    msgs5 = _simple_query(sock,
        "SELECT sales.id_customer, first_name, price FROM sales "
        "INNER JOIN customers ON sales.id_customer = customers.id_customer LIMIT 5")
    cols5 = []
    rd5   = next((body for t, body in msgs5 if t == 'T'), None)
    if rd5:
        cols5 = _parse_row_description(rd5)
    rows5 = _parse_data_rows(msgs5)
    ok("Second JOIN query succeeds (manager cache reuse)",
       len(rows5) > 0, f"rows={len(rows5)}, cols={cols5}")
except Exception as e:
    ok("Second JOIN query succeeds (manager cache reuse)", False, str(e))

# ---------------------------------------------------------------------------
# Test 12: COUNT(*) over JOIN
# ---------------------------------------------------------------------------
try:
    msgs6 = _simple_query(sock,
        "SELECT COUNT(*) AS n FROM sales "
        "INNER JOIN customers ON sales.id_customer = customers.id_customer")
    rows6 = _parse_data_rows(msgs6)
    n = int(rows6[0][0]) if rows6 and rows6[0][0] else 0
    ok("COUNT(*) over JOIN returns scalar > 0",
       len(rows6) == 1 and n > 0, f"n={n}")
except Exception as e:
    ok("COUNT(*) over JOIN returns scalar > 0", False, str(e))

# ---------------------------------------------------------------------------
# Test 13: Stop server cleanly
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
# Cleanup
# ---------------------------------------------------------------------------
shutil.rmtree(tmp_dir, ignore_errors=True)

# ---------------------------------------------------------------------------
print(f"\n{'='*50}")
print(f"  RESULTS: {passed} passed, {failed} failed")
if failed == 0:
    print("  ALL TESTS PASSED - Gap 24 INNER JOIN via PG Server ✓")
else:
    print(f"  {failed} FAILED")
    sys.exit(1)
