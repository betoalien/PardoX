from __future__ import annotations
"""
pardox_server/pg_server.py

Pure-Python PostgreSQL v3 wire protocol server backed by pardox_cpu.
Handles:
  - SSLRequest -> 'N' (not supported)
  - StartupMessage -> cleartext password auth
  - Simple Query ('Q') -> executes against registered .prdx/.parquet files
  - Extended Query ('P','B','D','E','S','H','C') -> minimal stubs
  - Terminate ('X')
"""
import os
import re
import socket
import struct
import threading
from datetime import datetime

from .engine  import PardoxEngine
from .registry import Registry


# ── PostgreSQL type OIDs (text wire format) ────────────────────────────────
_TYPE_OID = {
    "Utf8":      25,    # TEXT
    "Int64":     20,    # INT8
    "Float64":   701,   # FLOAT8
    "Boolean":   16,    # BOOL
    "Date":      1082,  # DATE
    "Timestamp": 1114,  # TIMESTAMP
    "Decimal":   1700,  # NUMERIC
    "Json":      25,    # TEXT
}
_TYPE_SIZE = {
    "Int64":    8,
    "Float64":  8,
    "Boolean":  1,
    "Date":     4,
    "Timestamp": 8,
}

# System-level queries that psql/JDBC send on startup (fallback — returns empty)
_CATALOG_PATTERNS = re.compile(
    r'pg_catalog\.|pg_type\b|pg_namespace\b|pg_class\b|pg_attribute\b'
    r'|information_schema\.|pg_proc\b|pg_description\b|pg_index\b'
    r'|pg_constraint\b|pg_roles\b|pg_user\b|pg_tables\b|pg_views\b',
    re.IGNORECASE,
)

# Specific catalog detectors (checked BEFORE the fallback)
_RE_PG_DATABASE  = re.compile(r'\bpg_database\b',  re.IGNORECASE)
_RE_PG_NAMESPACE = re.compile(r'\bpg_namespace\b', re.IGNORECASE)
_RE_PG_CLASS     = re.compile(r'\bpg_class\b|\bpg_tables\b', re.IGNORECASE)
_RE_PG_ATTRIBUTE = re.compile(r'\bpg_attribute\b', re.IGNORECASE)
_RE_PG_TYPE      = re.compile(r'\bpg_type\b',      re.IGNORECASE)


def _catalog_pg_database() -> tuple:
    """Return a fake 'postgres' database row — required by DBeaver/pgAdmin on connect."""
    cols = [
        ('oid', 26, 4), ('datname', 25, -1), ('datdba', 26, 4),
        ('encoding', 23, 4), ('datcollate', 25, -1), ('datctype', 25, -1),
        ('datistemplate', 16, 1), ('datallowconn', 16, 1), ('datconnlimit', 23, 4),
        ('datlastsysoid', 26, 4), ('datfrozenxid', 25, -1), ('dattablespace', 26, 4),
        ('datacl', 25, -1), ('datminmxid', 25, -1), ('rolname', 25, -1),
        ('char_encoding', 25, -1), ('description', 25, -1),
    ]
    rows = [['16384', 'postgres', '10', '6', 'en_US.UTF-8', 'en_US.UTF-8',
             'false', 'true', '-1', '14478', '726', '1663',
             None, '1', 'pardox_user', 'UTF8', None]]
    return cols, rows, "SELECT 1"


def _catalog_pg_namespace() -> tuple:
    """Return 'public' schema — required by DBeaver schema listing."""
    cols = [('oid', 26, 4), ('nspname', 25, -1), ('nspowner', 26, 4),
            ('nspacl', 25, -1), ('description', 25, -1)]
    rows = [['2200', 'public', '10', None, None]]
    return cols, rows, "SELECT 1"


def _catalog_pg_class(registry) -> tuple:
    """Return registered tables — required by DBeaver table listing."""
    cols = [
        ('oid', 26, 4), ('relname', 25, -1), ('relnamespace', 26, 4),
        ('relkind', 18, 1), ('relowner', 26, 4), ('relpages', 23, 4),
        ('reltuples', 701, 8), ('relhasindex', 16, 1), ('relisshared', 16, 1),
        ('relpersistence', 18, 1), ('relreplident', 18, 1),
        ('relhasrules', 16, 1), ('relhastriggers', 16, 1),
        ('relhassubclass', 16, 1), ('relacl', 25, -1), ('description', 25, -1),
        ('schemaname', 25, -1), ('tablename', 25, -1),
    ]
    rows = []
    for i, name in enumerate(registry.names()):
        oid = str(10000 + i)
        rows.append([oid, name, '2200', 'r', '10', '100',
                     '-1', 'false', 'false', 'p', 'd',
                     'false', 'false', 'false', None, None,
                     'public', name])
    return cols, rows, f"SELECT {len(rows)}"


def _catalog_pg_attribute(sql: str, registry, engine) -> tuple:
    """Return columns for a specific table — required by DBeaver column listing."""
    cols = [
        ('attrelid', 26, 4), ('attname', 25, -1), ('atttypid', 26, 4),
        ('attstattarget', 23, 4), ('attlen', 21, 2), ('attnum', 21, 2),
        ('attndims', 23, 4), ('attcacheoff', 23, 4), ('atttypmod', 23, 4),
        ('attbyval', 16, 1), ('attstorage', 18, 1), ('attalign', 18, 1),
        ('attnotnull', 16, 1), ('atthasdef', 16, 1), ('attidentity', 18, 1),
        ('attgenerated', 18, 1), ('attisdropped', 16, 1), ('attislocal', 16, 1),
        ('attinhcount', 23, 4), ('attcollation', 26, 4),
        ('attacl', 25, -1), ('attoptions', 25, -1), ('attfdwoptions', 25, -1),
        ('description', 25, -1), ('typname', 25, -1),
    ]
    # Try to extract table OID from the SQL (e.g., WHERE attrelid = 10000)
    m = re.search(r'attrelid\s*=\s*(\d+)', sql, re.IGNORECASE)
    if not m:
        return cols, [], "SELECT 0"
    oid = int(m.group(1))
    # Map OID back to table name
    names = registry.names()
    idx = oid - 10000
    if idx < 0 or idx >= len(names):
        return cols, [], "SELECT 0"
    table_name = names[idx]
    file_path = registry.get(table_name)
    if not file_path:
        return cols, [], "SELECT 0"
    # Load schema from pardox_cpu
    ext = os.path.splitext(file_path)[1].lower()
    mgr = engine.load_prdx(file_path) if ext == '.prdx' else engine.load_parquet(file_path)
    if not mgr:
        return cols, [], "SELECT 0"
    schema = engine.get_schema(mgr)
    _TYPE_OID_MAP = {
        'Utf8': 25, 'Json': 25, 'Int64': 20, 'Float64': 701,
        'Boolean': 16, 'Date': 1082, 'Timestamp': 1114, 'Decimal': 1700,
    }
    rows = []
    for attnum, (col_name, col_type) in enumerate(schema, start=1):
        type_oid = str(_TYPE_OID_MAP.get(col_type, 25))
        rows.append([str(oid), col_name, type_oid,
                     '-1', '-1', str(attnum),
                     '0', '-1', '-1',
                     'false', 'x', 'i',
                     'false', 'false', '', '',
                     'false', 'true', '0', '0',
                     None, None, None, None, col_type])
    return cols, rows, f"SELECT {len(rows)}"


# ── Wire-format builders ───────────────────────────────────────────────────

def _pack_msg(msg_type: bytes, body: bytes) -> bytes:
    length = len(body) + 4
    return msg_type + struct.pack('>I', length) + body


def _build_parameter_status(name: str, value: str) -> bytes:
    body = name.encode() + b'\x00' + value.encode() + b'\x00'
    return _pack_msg(b'S', body)


def _build_error_response(message: str, severity: str = "ERROR") -> bytes:
    body = (b'S' + severity.encode() + b'\x00'
            + b'M' + message.encode() + b'\x00'
            + b'C' + b'42000\x00'
            + b'\x00')
    return _pack_msg(b'E', body)


def _build_row_description(columns: list) -> bytes:
    """columns: list of (name, type_oid, type_size)"""
    body = struct.pack('>H', len(columns))
    for name, type_oid, type_size in columns:
        body += name.encode() + b'\x00'
        # I=table_oid H=col_attr I=type_oid h=type_size(signed) i=type_mod h=fmt
        body += struct.pack('>IHIhih', 0, 0, type_oid, type_size, -1, 0)
    return _pack_msg(b'T', body)


def _build_data_row(values: list) -> bytes:
    """values: list of str-or-None"""
    body = struct.pack('>H', len(values))
    for v in values:
        if v is None:
            body += struct.pack('>i', -1)
        else:
            encoded = str(v).encode('utf-8')
            body += struct.pack('>i', len(encoded)) + encoded
    return _pack_msg(b'D', body)


def _build_command_complete(tag: str) -> bytes:
    return _pack_msg(b'C', tag.encode() + b'\x00')


_READY_FOR_QUERY = b'Z\x00\x00\x00\x05I'
_BACKEND_KEY_DATA = b'K\x00\x00\x00\x0c\x00\x00\x00\x01\x00\x00\x00\x00'


# ── Low-level socket helpers ───────────────────────────────────────────────

def _recv_exact(sock: socket.socket, n: int) -> bytes | None:
    if n <= 0:
        return b''
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def _extract_param(data: bytes, key: bytes) -> str:
    """Extract a value from PostgreSQL startup key=value pairs."""
    needle = key + b'\x00'
    idx = data.find(needle)
    if idx == -1:
        return ''
    start = idx + len(needle)
    end = data.find(b'\x00', start)
    return data[start:end].decode('utf-8', errors='replace') if end != -1 else ''


# ── Value serialisation ────────────────────────────────────────────────────

def _val_to_text(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return 'true' if v else 'false'
    if isinstance(v, float):
        if v != v:       # NaN
            return 'NaN'
        if v == float('inf'):
            return 'Infinity'
        if v == float('-inf'):
            return '-Infinity'
        # Use repr for precision; avoid scientific notation for most values
        s = repr(v)
        return s
    return str(v)


# ── SQL rewriter ───────────────────────────────────────────────────────────

def _rewrite_sql(sql: str, table_names: list[str]) -> tuple[str | None, str | None]:
    """
    Replace the first matching registered table name with 't'.
    Handles: public.table_name  or  table_name
    Returns (table_name, rewritten_sql) or (None, None).
    """
    for name in table_names:
        escaped = re.escape(name)
        # Try public.<name> first
        pattern = r'(?i)\bpublic\.' + escaped + r'\b'
        if re.search(pattern, sql, re.IGNORECASE):
            return name, re.sub(pattern, 't', sql, flags=re.IGNORECASE)
        # Try bare <name>
        pattern = r'(?i)\b' + escaped + r'\b'
        if re.search(pattern, sql, re.IGNORECASE):
            return name, re.sub(pattern, 't', sql, flags=re.IGNORECASE)
    return None, None


# Default row cap for interactive SELECT queries (same as DBeaver/pgAdmin default).
# Aggregate-only queries (COUNT/SUM/AVG/MIN/MAX + GROUP BY) always run on full data.
_DEFAULT_ROW_LIMIT = 1_000

# Hard upper limit: even if the SQL engine returns more rows than expected,
# Python will cap the result before JSON serialisation to prevent OOM.
_HARD_ROW_CAP = 2_000

_AGG_PATTERN       = re.compile(r'\b(COUNT|SUM|AVG|MIN|MAX|MEAN|STD)\s*\(', re.IGNORECASE)
_GROUP_BY_PATTERN  = re.compile(r'\bGROUP\s+BY\b',   re.IGNORECASE)
_LIMIT_PATTERN     = re.compile(r'\bLIMIT\s+(\d+)',   re.IGNORECASE)
_WHERE_PATTERN     = re.compile(r'\bWHERE\b',         re.IGNORECASE)
_ORDER_BY_PATTERN  = re.compile(r'\bORDER\s+BY\b',    re.IGNORECASE)
_COUNT_STAR_PATTERN = re.compile(
    r'^\s*SELECT\s+COUNT\s*\(\s*\*\s*\)(?:\s+AS\s+(\w+))?\s+FROM\s+(?:\w+\.)?\w+\s*$',
    re.IGNORECASE,
)
_SCALAR_AGG_PATTERN = re.compile(
    r'^\s*SELECT\s+(MIN|MAX|AVG|SUM|MEAN)\s*\(\s*(\w+)\s*\)(?:\s+AS\s+(\w+))?\s+FROM\s+(?:\w+\.)?\w+\s*$',
    re.IGNORECASE,
)
# Gap-24: detect two-table INNER JOIN
_JOIN_PATTERN = re.compile(
    r'\bFROM\s+((?:\w+\.)*\w+)\s+(?:INNER\s+)?JOIN\s+((?:\w+\.)*\w+)\s+ON\s+'
    r'((?:\w+\.)*\w+)\s*=\s*((?:\w+\.)*\w+)',
    re.IGNORECASE,
)


def _inject_default_limit(sql: str) -> str:
    """Append LIMIT 10000 to plain SELECT queries that have no LIMIT and no aggregation."""
    if _LIMIT_PATTERN.search(sql):
        return sql   # already has LIMIT
    if _AGG_PATTERN.search(sql) or _GROUP_BY_PATTERN.search(sql):
        return sql   # aggregate / group-by — use full dataset
    return sql + f' LIMIT {_DEFAULT_ROW_LIMIT}'


# ── Streaming prdx helpers (avoid full file load) ──────────────────────────

def _try_streaming_prdx(engine, file_path: str, sql: str):
    """
    Try to answer a .prdx query without loading the full file into RAM.

    Handles:
      - COUNT(*)                            → pardox_prdx_count
      - SELECT scalar_agg(col) FROM t       → pardox_prdx_min/max/mean
      - SELECT * FROM t LIMIT N (no filter) → pardox_load_manager_prdx(path, N)
      - SELECT cols FROM t LIMIT N          → same limited load

    Returns (columns, rows, tag) if handled, or None to fall through to full load.
    """
    s = sql.strip()
    upper = s.upper()

    has_where    = bool(_WHERE_PATTERN.search(upper))
    has_group    = bool(_GROUP_BY_PATTERN.search(upper))
    has_order    = bool(_ORDER_BY_PATTERN.search(upper))
    has_agg      = bool(_AGG_PATTERN.search(upper))
    limit_match  = _LIMIT_PATTERN.search(upper)
    limit_n      = int(limit_match.group(1)) if limit_match else None

    # ── COUNT(*) without WHERE / GROUP BY ────────────────────────────────────
    _count_m = _COUNT_STAR_PATTERN.match(s)
    if _count_m and not has_where and not has_group:
        alias = _count_m.group(1) or 'count'
        count = engine.prdx_count(file_path)
        if count >= 0:
            cols = [(alias, _TYPE_OID['Int64'], _TYPE_SIZE['Int64'])]
            return cols, [[str(count)]], 'SELECT 1'

    # ── Single scalar agg without WHERE / GROUP BY ───────────────────────────
    m = _SCALAR_AGG_PATTERN.match(s)
    if m and not has_where and not has_group:
        func, col = m.group(1).upper(), m.group(2)
        alias = m.group(3) or f'{func.lower()}_{col}'
        val = engine.prdx_scalar(file_path, col, func)
        cols = [(alias, _TYPE_OID['Float64'], _TYPE_SIZE['Float64'])]
        return cols, [[_val_to_text(val)]], 'SELECT 1'

    # ── SELECT [cols] FROM t LIMIT N  (no WHERE, no ORDER BY, no GROUP BY) ───
    # Only load the rows we actually need — no full file scan.
    if limit_n is not None and not has_where and not has_order and not has_group and not has_agg:
        mgr = engine.load_prdx_limited(file_path, limit_n)
        if not mgr:
            return [], [], 'SELECT 0'
        result_mgr = engine.sql_query(mgr, sql)
        engine.free_manager(mgr)   # free limited load — NOT the cache
        if not result_mgr:
            return [], [], 'SELECT 0'
        schema  = engine.get_schema(result_mgr)
        records = engine.get_records(result_mgr)
        row_cnt = engine.get_row_count(result_mgr)
        engine.free_manager(result_mgr)
        columns = [(n, _TYPE_OID.get(t, 25), _TYPE_SIZE.get(t, -1)) for n, t in schema]
        col_names = [n for n, _, _ in columns]
        rows = [[_val_to_text(rec.get(c)) for c in col_names] for rec in records]
        return columns, rows, f'SELECT {row_cnt}'

    # ── SELECT [cols] FROM t ORDER BY x LIMIT N (no WHERE, no GROUP BY) ─────
    # Load a safe overcount so the ORDER BY + LIMIT has enough rows to sort.
    if limit_n is not None and has_order and not has_where and not has_group and not has_agg:
        safe_rows = min(limit_n * 200, 500_000)
        mgr = engine.load_prdx_limited(file_path, safe_rows)
        if not mgr:
            return [], [], 'SELECT 0'
        result_mgr = engine.sql_query(mgr, sql)
        engine.free_manager(mgr)
        if not result_mgr:
            return [], [], 'SELECT 0'
        schema  = engine.get_schema(result_mgr)
        records = engine.get_records(result_mgr)
        row_cnt = engine.get_row_count(result_mgr)
        engine.free_manager(result_mgr)
        columns = [(n, _TYPE_OID.get(t, 25), _TYPE_SIZE.get(t, -1)) for n, t in schema]
        col_names = [n for n, _, _ in columns]
        rows = [[_val_to_text(rec.get(c)) for c in col_names] for rec in records]
        return columns, rows, f'SELECT {row_cnt}'

    # ── Streaming GROUP BY (no WHERE) — routes to pardox_groupby_agg_prdx ────
    # Avoids full-file load for GROUP BY + aggregates on large .prdx files.
    if has_group and has_agg and not has_where:
        group_m = re.search(
            r'\bGROUP\s+BY\s+(.+?)(?:\s+HAVING\b|\s+ORDER\s+BY\b|\s*$)',
            s, re.IGNORECASE | re.DOTALL,
        )
        select_m = re.search(r'\bSELECT\s+(.+?)\s+FROM\b', s, re.IGNORECASE | re.DOTALL)
        if group_m and select_m:
            group_cols = [
                c.strip().strip('"').strip('`')
                for c in group_m.group(1).strip().split(',')
            ]
            agg_specs: dict = {}
            for agg_m2 in re.finditer(
                r'\b(SUM|AVG|MIN|MAX|MEAN)\s*\(\s*(\w+)\s*\)',
                select_m.group(1), re.IGNORECASE,
            ):
                agg_specs[agg_m2.group(2)] = agg_m2.group(1).lower()
            if group_cols and agg_specs:
                mgr = engine.prdx_groupby(file_path, group_cols, agg_specs)
                if not mgr:
                    return [], [], 'SELECT 0'
                # Apply ORDER BY / LIMIT on the groupby result if present
                post_m = re.search(
                    r'(ORDER\s+BY\b.+|LIMIT\s+\d+)$', s,
                    re.IGNORECASE | re.DOTALL,
                )
                if post_m:
                    post_mgr = engine.sql_query(mgr, f'SELECT * FROM t {post_m.group(1)}')
                    engine.free_manager(mgr)
                    if not post_mgr:
                        return [], [], 'SELECT 0'
                    mgr = post_mgr
                schema  = engine.get_schema(mgr)
                records = engine.get_records(mgr)
                row_cnt = engine.get_row_count(mgr)
                engine.free_manager(mgr)
                columns = [(n, _TYPE_OID.get(tp, 25), _TYPE_SIZE.get(tp, -1))
                           for n, tp in schema]
                col_names = [n for n, _, _ in columns]
                rows = [[_val_to_text(rec.get(c)) for c in col_names] for rec in records]
                return columns, rows, f'SELECT {row_cnt}'

    # ── WHERE + LIMIT best-effort streaming (Gap 25) ──────────────────────────
    # Load a chunk large enough for WHERE to produce LIMIT results (best-effort).
    if limit_n is not None and has_where and not has_group and not has_agg:
        safe_rows = min(limit_n * 500, 500_000)
        mgr = engine.load_prdx_limited(file_path, safe_rows)
        if not mgr:
            return [], [], 'SELECT 0'
        result_mgr = engine.sql_query(mgr, s)
        engine.free_manager(mgr)
        if not result_mgr:
            return [], [], 'SELECT 0'
        schema  = engine.get_schema(result_mgr)
        records = engine.get_records(result_mgr)
        row_cnt = engine.get_row_count(result_mgr)
        engine.free_manager(result_mgr)
        columns = [(n, _TYPE_OID.get(tp, 25), _TYPE_SIZE.get(tp, -1))
                   for n, tp in schema]
        col_names = [n for n, _, _ in columns]
        rows = [[_val_to_text(rec.get(c)) for c in col_names] for rec in records]
        return columns, rows, f'SELECT {row_cnt}'

    # Fall through → caller will attempt full cached load
    return None


# ── Gap-24: JOIN helpers ────────────────────────────────────────────────────

def _parse_join(sql: str, table_names: list) -> tuple | None:
    """
    Parse a two-table INNER JOIN query.
    Returns (left_table, right_table, left_key, right_key, rewritten_sql) or None.
    """
    m = _JOIN_PATTERN.search(sql)
    if not m:
        return None

    left_full      = m.group(1)   # e.g. 'public.sales' or 'sales'
    right_full     = m.group(2)   # e.g. 'customers'
    left_key_expr  = m.group(3)   # e.g. 'sales.id_customer'
    right_key_expr = m.group(4)   # e.g. 'customers.id_customer'

    left_bare  = left_full.split('.')[-1]
    right_bare = right_full.split('.')[-1]

    def find_reg(bare: str) -> str:
        for name in table_names:
            if name.lower() == bare.lower():
                return name
        return bare

    left_table  = find_reg(left_bare)
    right_table = find_reg(right_bare)
    left_key    = left_key_expr.split('.')[-1]
    right_key   = right_key_expr.split('.')[-1]

    # Rewrite SQL so the join result can be queried as a single table 't'
    rewritten = sql
    # Strip schema prefix (public.)
    rewritten = re.sub(r'\bpublic\.', '', rewritten, flags=re.IGNORECASE)
    # Strip table name prefixes from column references (e.g. sales.amount → amount)
    for bare in {left_bare, right_bare}:
        rewritten = re.sub(
            r'\b' + re.escape(bare) + r'\.',
            '', rewritten, flags=re.IGNORECASE,
        )
    # Collapse "FROM left [INNER] JOIN right ON key = key" → "FROM t"
    rewritten = _JOIN_PATTERN.sub('FROM t', rewritten, count=1)

    return left_table, right_table, left_key, right_key, rewritten


def _execute_join(engine, registry, sql: str, table_names: list) -> tuple | None:
    """
    Execute a two-table INNER JOIN query (Gap 24).
    Returns (columns, rows, tag) or None if the SQL isn't a supported JOIN.
    """
    parsed = _parse_join(sql, table_names)
    if parsed is None:
        return None

    left_table, right_table, left_key, right_key, rewritten = parsed

    left_path  = registry.get(left_table)
    right_path = registry.get(right_table)
    if not left_path or not right_path:
        return [], [], 'SELECT 0'

    def _load(path: str) -> int:
        ext = os.path.splitext(path)[1].lower()
        if ext == '.prdx':
            return engine.load_prdx(path)
        if ext == '.parquet':
            return engine.load_parquet(path)
        return 0

    left_mgr  = _load(left_path)
    right_mgr = _load(right_path)
    if not left_mgr or not right_mgr:
        return [], [], 'SELECT 0'

    joined_mgr = engine.hash_join(left_mgr, right_mgr, left_key, right_key)
    if not joined_mgr:
        return [], [], 'SELECT 0'

    rewritten  = _inject_default_limit(rewritten)
    result_mgr = engine.sql_query(joined_mgr, rewritten)
    engine.free_manager(joined_mgr)
    if not result_mgr:
        return [], [], 'SELECT 0'

    schema    = engine.get_schema(result_mgr)
    records   = engine.get_records(result_mgr)
    row_cnt   = engine.get_row_count(result_mgr)
    engine.free_manager(result_mgr)

    columns   = [(n, _TYPE_OID.get(tp, 25), _TYPE_SIZE.get(tp, -1)) for n, tp in schema]
    col_names = [n for n, _, _ in columns]
    rows      = [[_val_to_text(rec.get(c)) for c in col_names] for rec in records]
    return columns, rows, f'SELECT {row_cnt}'


# ── Query executor ─────────────────────────────────────────────────────────

class QueryHandler:
    """Executes SQL against registered tables via pardox_cpu."""

    def __init__(self, engine: PardoxEngine, registry: Registry):
        self._engine   = engine
        self._registry = registry

    def execute(self, sql: str) -> tuple:
        """
        Returns (columns, rows, command_tag) where:
          columns: list[(name, type_oid, type_size)]  -- empty for DDL/SET/etc.
          rows:    list[list[str|None]]
          command_tag: e.g. "SELECT 5", "SET", "BEGIN"
        """
        sql_s = sql.strip().rstrip(';').strip()
        upper = sql_s.upper()

        # ── Empty ──────────────────────────────────────────────────────────
        if not sql_s:
            return [], [], "SELECT 0"

        # ── SET / config ───────────────────────────────────────────────────
        if upper.startswith('SET '):
            return [], [], "SET"

        # ── Transaction control ────────────────────────────────────────────
        if upper in ('BEGIN', 'START TRANSACTION', 'COMMIT', 'END', 'ROLLBACK',
                     'SAVEPOINT', 'RELEASE SAVEPOINT', 'ROLLBACK TO SAVEPOINT'):
            tag = upper.split()[0]
            return [], [], tag

        # ── SELECT version() ──────────────────────────────────────────────
        if re.match(r'SELECT\s+VERSION\s*\(\s*\)', upper):
            cols = [('version', 25, -1)]
            rows = [['PostgreSQL 14.0 (pardoX Server 0.3.2)']]
            return cols, rows, "SELECT 1"

        # ── SHOW ───────────────────────────────────────────────────────────
        if upper.startswith('SHOW '):
            param = sql_s[5:].strip().lower()
            fakes = {
                'server_version': '14.0',
                'server_version_num': '140000',
                'client_encoding': 'UTF8',
                'server_encoding': 'UTF8',
                'datestyle': 'ISO, MDY',
                'timezone': 'UTC',
                'transaction isolation level': 'read committed',
                'standard_conforming_strings': 'on',
                'integer_datetimes': 'on',
            }
            val = fakes.get(param, '')
            cols = [(param, 25, -1)]
            rows = [[val]]
            return cols, rows, "SHOW"

        # ── pg_catalog.set_config(...) ─────────────────────────────────────
        if 'SET_CONFIG' in upper:
            # Returns the new value (third argument of the call)
            m = re.search(r"set_config\s*\(\s*'[^']*'\s*,\s*'([^']*)'\s*,", sql_s, re.IGNORECASE)
            val = m.group(1) if m else ''
            cols = [('set_config', 25, -1)]
            rows = [[val]]
            return cols, rows, "SELECT 1"

        # ── current_database() / current_schema() / current_user / session_user ─
        # DBeaver/psql send these as part of the connection handshake.
        if re.match(r'SELECT\s+CURRENT_DATABASE\s*\(\s*\)\s*,', upper):
            # Multi-function: SELECT current_database(), current_schema(), current_user
            return [
                ('current_database', 25, -1), ('current_schema', 25, -1),
                ('current_user', 25, -1),
            ], [['postgres', 'public', 'pardox_user']], "SELECT 1"
        if re.match(r'SELECT\s+CURRENT_DATABASE\s*\(\s*\)', upper):
            return [('current_database', 25, -1)], [['postgres']], "SELECT 1"
        if re.match(r'SELECT\s+CURRENT_SCHEMA\s*\(\s*\)', upper):
            return [('current_schema', 25, -1)], [['public']], "SELECT 1"
        if re.match(r'SELECT\s+CURRENT_USER', upper):
            return [('current_user', 25, -1)], [['pardox_user']], "SELECT 1"
        if re.match(r'SELECT\s+SESSION_USER', upper):
            return [('session_user', 25, -1)], [['pardox_user']], "SELECT 1"
        if re.match(r'SELECT\s+USER\b', upper):
            return [('user', 25, -1)], [['pardox_user']], "SELECT 1"

        # ── Specific catalog handlers (DBeaver / pgAdmin / Tableau need these) ─
        # These are checked BEFORE the general empty-catalog fallback below.
        if _RE_PG_DATABASE.search(sql_s):
            return _catalog_pg_database()
        if _RE_PG_NAMESPACE.search(sql_s):
            return _catalog_pg_namespace()
        if _RE_PG_ATTRIBUTE.search(sql_s):
            return _catalog_pg_attribute(sql_s, self._registry, self._engine)
        if _RE_PG_CLASS.search(sql_s):
            return _catalog_pg_class(self._registry)

        # ── System / catalog queries → empty result (fallback) ─────────────
        if _CATALOG_PATTERNS.search(sql_s):
            return [], [], "SELECT 0"

        # ── Try registered tables ──────────────────────────────────────────
        table_names = self._registry.names()

        # ── JOIN queries (Gap 24) ─────────────────────────────────────────
        if _JOIN_PATTERN.search(sql_s):
            join_result = _execute_join(
                self._engine, self._registry, sql_s, table_names
            )
            if join_result is not None:
                return join_result

        table_name, rewritten = _rewrite_sql(sql_s, table_names)
        if table_name is None:
            # Unknown table — return empty
            return [], [], "SELECT 0"

        file_path = self._registry.get(table_name)
        if not file_path:
            return [], [], "SELECT 0"

        # Inject default row cap for non-aggregate SELECT without explicit LIMIT.
        rewritten = _inject_default_limit(rewritten)

        ext = os.path.splitext(file_path)[1].lower()

        # ── SMART ROUTING for .prdx files ────────────────────────────────────
        # Avoids loading the entire file into RAM for common interactive queries.
        # Falls back to full cached load only when streaming isn't sufficient.
        if ext == '.prdx':
            result = _try_streaming_prdx(self._engine, file_path, rewritten)
            if result is not None:
                return result   # (columns, rows, tag) already assembled

        # ── Standard path: load into memory then query ────────────────────────
        if ext == '.prdx':
            # Safety check: estimate row count before attempting full load
            approx = self._engine.prdx_row_count_approx(file_path)
            if approx > self._engine._SAFE_LOAD_ROWS:
                return (
                    [('error', 25, -1)],
                    [[f'Table is too large for full scan ({approx:,} est. rows). '
                      f'Use COUNT(*), LIMIT, or GROUP BY aggregates.']],
                    'SELECT 1',
                )
            mgr = self._engine.load_prdx(file_path)
        elif ext == '.parquet':
            mgr = self._engine.load_parquet(file_path)
        else:
            return [], [], "SELECT 0"

        if not mgr:
            return [], [], "SELECT 0"

        result_mgr = self._engine.sql_query(mgr, rewritten)
        # Do NOT free mgr — it is owned by the engine's cache and reused across queries.

        if not result_mgr:
            return [], [], "SELECT 0"

        row_count = self._engine.get_row_count(result_mgr)

        # Hard row cap: truncate BEFORE JSON serialisation to prevent OOM.
        if row_count > _HARD_ROW_CAP:
            capped_mgr = self._engine.sql_query(result_mgr, f'SELECT * FROM t LIMIT {_HARD_ROW_CAP}')
            self._engine.free_manager(result_mgr)
            if not capped_mgr:
                return [], [], "SELECT 0"
            result_mgr = capped_mgr
            row_count  = _HARD_ROW_CAP

        schema    = self._engine.get_schema(result_mgr)
        records   = self._engine.get_records(result_mgr)
        self._engine.free_manager(result_mgr)

        columns = [
            (name, _TYPE_OID.get(typ, 25), _TYPE_SIZE.get(typ, -1))
            for name, typ in schema
        ]
        col_names = [name for name, _, _ in columns]
        rows = [
            [_val_to_text(rec.get(c)) for c in col_names]
            for rec in records
        ]
        return columns, rows, f"SELECT {row_count}"


# ── Connection handler ─────────────────────────────────────────────────────

class _ConnectionHandler:
    def __init__(self, conn: socket.socket, addr, username: str, password: str,
                 query_handler: QueryHandler, log_fn, stop_event: threading.Event):
        self._conn   = conn
        self._addr   = addr
        self._user   = username
        self._pass   = password
        self._qh     = query_handler
        self._log    = log_fn
        self._stop   = stop_event
        # Extended Query state (per-connection)
        self._statements: dict[str, str]   = {}   # stmt_name -> SQL text
        self._portals:    dict[str, tuple] = {}    # portal_name -> (cols, rows, tag)

    def run(self):
        try:
            conn = self._conn
            conn.settimeout(300)

            # ── Initial 8 bytes ────────────────────────────────────────────
            initial = _recv_exact(conn, 8)
            if not initial:
                return
            msg_len = struct.unpack('>I', initial[:4])[0]
            code    = struct.unpack('>I', initial[4:8])[0]

            # ── SSLRequest? ────────────────────────────────────────────────
            if msg_len == 8 and code == 80877103:
                conn.sendall(b'N')
                hdr = _recv_exact(conn, 8)
                if not hdr:
                    return
                startup_len = struct.unpack('>I', hdr[:4])[0]
                rest = _recv_exact(conn, startup_len - 8)
                startup_data = hdr + (rest or b'')
            else:
                rest = _recv_exact(conn, msg_len - 8)
                startup_data = initial + (rest or b'')

            # ── Parse StartupMessage ───────────────────────────────────────
            params = startup_data[8:]      # skip length + protocol version
            attempted_user = _extract_param(params, b'user')
            self._log(f"[CONN] New connection from {self._addr[0]} user='{attempted_user}'")

            # ── Request cleartext password ─────────────────────────────────
            conn.sendall(b'R\x00\x00\x00\x08\x00\x00\x00\x03')

            # ── Read PasswordMessage ───────────────────────────────────────
            ph = _recv_exact(conn, 5)
            if not ph or ph[0:1] != b'p':
                conn.sendall(_build_error_response("unexpected message during auth"))
                return
            pass_len = struct.unpack('>I', ph[1:5])[0]
            pass_body = _recv_exact(conn, pass_len - 4) or b''
            attempted_pass = pass_body.rstrip(b'\x00').decode('utf-8', errors='replace')

            # ── Validate ───────────────────────────────────────────────────
            if attempted_user != self._user or attempted_pass != self._pass:
                self._log(f"[AUTH] FAILED user='{attempted_user}'")
                conn.sendall(_build_error_response(
                    f"password authentication failed for user \"{attempted_user}\"",
                    "FATAL"
                ))
                return

            self._log(f"[AUTH] SUCCESS user='{attempted_user}'")

            # ── Auth OK + server params ────────────────────────────────────
            conn.sendall(b'R\x00\x00\x00\x08\x00\x00\x00\x00')  # AuthenticationOk
            for k, v in [
                ('server_version', '14.0'),
                ('client_encoding', 'UTF8'),
                ('server_encoding', 'UTF8'),
                ('DateStyle', 'ISO, MDY'),
                ('TimeZone', 'UTC'),
                ('integer_datetimes', 'on'),
                ('standard_conforming_strings', 'on'),
            ]:
                conn.sendall(_build_parameter_status(k, v))
            conn.sendall(_BACKEND_KEY_DATA)
            conn.sendall(_READY_FOR_QUERY)

            # ── Message loop ───────────────────────────────────────────────
            while not self._stop.is_set():
                try:
                    type_buf = _recv_exact(conn, 1)
                except OSError:
                    break
                if not type_buf:
                    break

                len_buf = _recv_exact(conn, 4)
                if not len_buf:
                    break
                body_len = struct.unpack('>I', len_buf)[0] - 4
                body = _recv_exact(conn, body_len) if body_len > 0 else b''
                if body is None:
                    break

                msg_type = chr(type_buf[0])

                if msg_type == 'X':          # Terminate
                    self._log("[CONN] Client terminated gracefully")
                    break

                elif msg_type == 'Q':        # Simple Query
                    sql = (body or b'').rstrip(b'\x00').decode('utf-8', errors='replace')
                    self._handle_simple_query(conn, sql)

                elif msg_type == 'P':        # Parse — store SQL for later execution
                    try:
                        end_n = body.index(b'\x00')
                        stmt_name = body[:end_n].decode('utf-8', errors='replace')
                        rest = body[end_n + 1:]
                        end_q = rest.index(b'\x00')
                        query = rest[:end_q].decode('utf-8', errors='replace')
                        self._statements[stmt_name] = query
                        if query.strip():
                            self._log(f"[PARSE] {query[:120]}")
                    except Exception:
                        pass
                    conn.sendall(b'1\x00\x00\x00\x04')  # ParseComplete

                elif msg_type == 'B':        # Bind — execute SQL, buffer result
                    try:
                        end_p = body.index(b'\x00')
                        portal_name = body[:end_p].decode('utf-8', errors='replace')
                        rest = body[end_p + 1:]
                        end_s = rest.index(b'\x00')
                        stmt_name = rest[:end_s].decode('utf-8', errors='replace')
                        # Reuse pre-executed result from Describe(S) if available
                        pre_key = f'\x00S\x00{stmt_name}'
                        if pre_key in self._portals:
                            self._portals[portal_name] = self._portals.pop(pre_key)
                        else:
                            sql = self._statements.get(stmt_name, '')
                            if sql:
                                self._log(f"[BIND ] {sql[:120]}")
                                cols, rows, tag = self._qh.execute(sql)
                            else:
                                cols, rows, tag = [], [], 'SELECT 0'
                            self._portals[portal_name] = (cols, rows, tag)
                    except Exception as e:
                        self._log(f"[WARN] Extended Bind: {e}")
                    conn.sendall(b'2\x00\x00\x00\x04')  # BindComplete

                elif msg_type == 'D':        # Describe portal or statement
                    try:
                        dtype = chr(body[0]) if body else 'P'
                        name  = body[1:].split(b'\x00')[0].decode('utf-8', errors='replace') if len(body) > 1 else ''
                        if dtype == 'S':
                            # Describe statement — pre-execute to get schema
                            sql = self._statements.get(name, '')
                            # ParameterDescription: 0 parameters
                            conn.sendall(b't\x00\x00\x00\x06\x00\x00')
                            if sql:
                                self._log(f"[DESC ] {sql[:120]}")
                                cols, rows, tag = self._qh.execute(sql)
                                self._portals[f'\x00S\x00{name}'] = (cols, rows, tag)
                                conn.sendall(_build_row_description(cols))
                            else:
                                conn.sendall(b'n\x00\x00\x00\x04')  # NoData
                        else:
                            # Describe portal
                            result = self._portals.get(name)
                            if result:
                                conn.sendall(_build_row_description(result[0]))
                            else:
                                conn.sendall(b'n\x00\x00\x00\x04')  # NoData
                    except Exception as e:
                        self._log(f"[WARN] Extended Describe: {e}")
                        conn.sendall(b'n\x00\x00\x00\x04')

                elif msg_type == 'E':        # Execute — stream rows from buffered portal
                    try:
                        end_p = body.index(b'\x00') if b'\x00' in body else len(body)
                        portal_name = body[:end_p].decode('utf-8', errors='replace')
                        max_rows = struct.unpack('>I', body[end_p+1:end_p+5])[0] if len(body) >= end_p + 5 else 0
                        result = self._portals.get(portal_name)
                        if result:
                            cols, rows, tag = result
                            out_rows = rows[:max_rows] if max_rows > 0 else rows
                            for row in out_rows:
                                conn.sendall(_build_data_row(row))
                            if max_rows > 0 and len(rows) > max_rows:
                                conn.sendall(b's\x00\x00\x00\x04')  # PortalSuspended
                            else:
                                conn.sendall(_build_command_complete(tag))
                        else:
                            conn.sendall(_build_command_complete('SELECT 0'))
                    except Exception as e:
                        self._log(f"[WARN] Extended Execute: {e}")
                        conn.sendall(_build_command_complete('SELECT 0'))

                elif msg_type == 'S':        # Sync
                    conn.sendall(_READY_FOR_QUERY)

                elif msg_type == 'H':        # Flush
                    pass

                elif msg_type == 'C':        # Close portal or statement
                    try:
                        ctype = chr(body[0]) if body else 'P'
                        name  = body[1:].split(b'\x00')[0].decode('utf-8', errors='replace') if len(body) > 1 else ''
                        if ctype == 'P':
                            self._portals.pop(name, None)
                        else:
                            self._statements.pop(name, None)
                    except Exception:
                        pass
                    conn.sendall(b'3\x00\x00\x00\x04')  # CloseComplete

                else:
                    self._log(f"[WARN] Unknown message type '{msg_type}' (0x{type_buf[0]:02x})")
                    conn.sendall(_READY_FOR_QUERY)

        except OSError:
            pass
        except Exception as e:
            self._log(f"[ERROR] Connection handler: {e}")
        finally:
            self._conn.close()

    def _handle_simple_query(self, conn: socket.socket, sql: str):
        self._log(f"[QUERY] {sql[:120]}")
        try:
            columns, rows, command_tag = self._qh.execute(sql)
        except Exception as e:
            self._log(f"[ERROR] {e}")
            conn.sendall(_build_error_response(str(e)))
            conn.sendall(_READY_FOR_QUERY)
            return

        if command_tag in ('SET', 'BEGIN', 'COMMIT', 'ROLLBACK', 'END',
                           'START TRANSACTION', 'SAVEPOINT', 'RELEASE SAVEPOINT'):
            conn.sendall(_build_command_complete(command_tag))
        elif command_tag == 'SHOW':
            conn.sendall(_build_row_description(columns))
            for row in rows:
                conn.sendall(_build_data_row(row))
            conn.sendall(_build_command_complete(command_tag))
        else:
            conn.sendall(_build_row_description(columns))
            for row in rows:
                conn.sendall(_build_data_row(row))
            conn.sendall(_build_command_complete(command_tag))
        conn.sendall(_READY_FOR_QUERY)


# ── Public server class ────────────────────────────────────────────────────

class PardoXPostgresServer:
    """Thread-backed PostgreSQL wire protocol server."""

    def __init__(self, engine: PardoxEngine, registry: Registry,
                 host: str = '0.0.0.0', port: int = 5235,
                 username: str = 'pardox_user', password: str = 'pardoX_secret'):
        self._engine   = engine
        self._registry = registry
        self._host     = host
        self._port     = port
        self._username = username
        self._password = password
        self._stop     = threading.Event()
        self._thread: threading.Thread | None = None
        self._logs: list[dict] = []
        self._logs_lock = threading.Lock()
        self._qh = QueryHandler(engine, registry)

    # ── logging ─────────────────────────────────────────────────────────────

    def _log(self, message: str):
        print(f"[pardox_server] {message}")
        with self._logs_lock:
            self._logs.append({
                'ts':  datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'msg': message,
            })
            if len(self._logs) > 1000:
                self._logs.pop(0)

    def get_logs(self) -> list[dict]:
        with self._logs_lock:
            return list(self._logs)

    # ── lifecycle ────────────────────────────────────────────────────────────

    def start(self) -> bool:
        if self._thread and self._thread.is_alive():
            return False
        self._stop.clear()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()
        return True

    def stop(self):
        self._stop.set()

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def reconfigure(self, port: int | None = None, username: str | None = None,
                    password: str | None = None):
        if port is not None:
            self._port = port
        if username is not None:
            self._username = username
        if password is not None:
            self._password = password

    # ── accept loop ──────────────────────────────────────────────────────────

    def _serve(self):
        try:
            srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((self._host, self._port))
            srv.listen(32)
            srv.settimeout(0.1)   # fast stop: checks _stop event 10x/second
            self._log(f"Listening on {self._host}:{self._port}")
        except OSError as e:
            self._log(f"[ERROR] Bind failed: {e}")
            return

        while not self._stop.is_set():
            try:
                conn, addr = srv.accept()
                handler = _ConnectionHandler(
                    conn, addr,
                    self._username, self._password,
                    self._qh, self._log, self._stop
                )
                t = threading.Thread(target=handler.run, daemon=True)
                t.start()
            except socket.timeout:
                continue
            except OSError:
                break

        srv.close()
        self._log("Server stopped")
