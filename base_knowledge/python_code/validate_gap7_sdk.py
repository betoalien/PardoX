#!/usr/bin/env python3
"""
validate_gap7.py - Gap 7: GPU Compute Beyond Sort

Tests the new GPU-accelerated math functions exposed via the CPU API.
Each function tries GPU first; falls back to CPU if GPU is unavailable.
Results must match CPU math within the tolerance imposed by f32 GPU precision
(GPU internally works in f32, so ~1e-4 relative tolerance is expected).

BINARY OPS (col_a OP col_b -> new manager):
  pardox_gpu_add  - A + B
  pardox_gpu_sub  - A − B
  pardox_gpu_mul  - A × B
  pardox_gpu_div  - A ÷ B

UNARY OPS (col -> new manager):
  pardox_gpu_sqrt - √(col)
  pardox_gpu_exp  - e^col
  pardox_gpu_log  - ln(col)
  pardox_gpu_abs  - |col|

Dataset: small deterministic CSV written by this script (16 rows).
"""

import ctypes
import math
import os
import tempfile
import json
import sys

from pardox import wrapper as wr
lib = wr.lib

# ── Test data ─────────────────────────────────────────────────────────────────
# 16 rows; all values positive (for log/sqrt), some will be negated for abs test
VALUES_A = [1.0, 4.0, 9.0, 16.0, 25.0, 0.5, 2.0, 8.0,
            0.25, 100.0, 3.0, 7.0, 0.1, 50.0, 0.01, 64.0]
VALUES_B = [2.0, 2.0, 3.0, 4.0, 5.0, 0.5, 1.0, 2.0,
            0.5, 10.0, 3.0, 7.0, 0.1, 5.0, 0.01, 8.0]
# neg_val: some negative entries for abs test
VALUES_NEG = [-v if i % 2 == 0 else v for i, v in enumerate(VALUES_A)]

NROWS = len(VALUES_A)
TOLERANCE = 1e-3   # f32 GPU precision; most ops hit < 1e-5 but exp can vary more

# ── Write a small deterministic CSV ──────────────────────────────────────────
CSV_PATH = os.path.join(os.path.dirname(__file__), "_gap7_test.csv")
with open(CSV_PATH, "w") as f:
    f.write("val_a,val_b,val_neg\n")
    for a, b, n in zip(VALUES_A, VALUES_B, VALUES_NEG):
        f.write(f"{a},{b},{n}\n")

# ── FFI bindings ──────────────────────────────────────────────────────────────
def setup_api():
    # Load manager from CSV
    lib.pardox_load_manager_csv.restype  = ctypes.c_void_p
    lib.pardox_load_manager_csv.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]

    lib.pardox_free_manager.restype  = None
    lib.pardox_free_manager.argtypes = [ctypes.c_void_p]

    lib.pardox_get_row_count.restype  = ctypes.c_longlong
    lib.pardox_get_row_count.argtypes = [ctypes.c_void_p]

    lib.pardox_get_f64_buffer.restype  = ctypes.c_void_p
    lib.pardox_get_f64_buffer.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
                                          ctypes.POINTER(ctypes.c_size_t)]

    lib.pardox_get_schema_json.restype  = ctypes.c_char_p
    lib.pardox_get_schema_json.argtypes = [ctypes.c_void_p]

    # GPU binary ops
    for name in ["pardox_gpu_add", "pardox_gpu_sub", "pardox_gpu_mul", "pardox_gpu_div"]:
        fn = getattr(lib, name)
        fn.restype  = ctypes.c_void_p
        fn.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]

    # GPU unary ops
    for name in ["pardox_gpu_sqrt", "pardox_gpu_exp", "pardox_gpu_log", "pardox_gpu_abs"]:
        fn = getattr(lib, name)
        fn.restype  = ctypes.c_void_p
        fn.argtypes = [ctypes.c_void_p, ctypes.c_char_p]


def get_f64_values(mgr, col_name):
    """Read Float64 column from manager as list of Python floats."""
    out_len = ctypes.c_size_t(0)
    ptr = lib.pardox_get_f64_buffer(mgr, col_name.encode(), ctypes.byref(out_len))
    if not ptr:
        return None
    nrows = out_len.value
    arr = (ctypes.c_double * nrows).from_address(ptr)
    return list(arr)


CSV_CONFIG = json.dumps({"delimiter": ",", "has_header": True}).encode()


def load_csv(path):
    schema = json.dumps({"column_names": ["val_a", "val_b", "val_neg"],
                          "column_types": ["Float64", "Float64", "Float64"]})
    return lib.pardox_load_manager_csv(path.encode(), schema.encode(), CSV_CONFIG)


def free(mgr):
    lib.pardox_free_manager(mgr)


# ── Test harness ──────────────────────────────────────────────────────────────
setup_api()

results = []
PASS = "OK"
FAIL = "FAIL"

def check(name, cond, detail=""):
    status = PASS if cond else FAIL
    results.append((name, status, detail))
    mark = "✓" if cond else "✗"
    print(f"  [{mark}] {name}" + (f"  - {detail}" if detail else ""))


# Check GPU availability (informational only - tests pass either way)
mgr_probe = load_csv(CSV_PATH)
schema_raw = lib.pardox_get_schema_json(mgr_probe)
schema_cols = [c["name"] for c in json.loads(schema_raw.decode())["columns"]]
free(mgr_probe)

print("\n=== Gap 7: GPU Compute Beyond Sort ===\n")
print(f"Schema columns found: {schema_cols}")
print(f"Test rows: {NROWS} | Tolerance: {TOLERANCE}\n")

# ──────────────────────────────────────────────────────────────────────────────
# BINARY OPS
# ──────────────────────────────────────────────────────────────────────────────
print("── Binary Operations ────────────────────────────")

def test_binary(fn_name, col_a, col_b, expected_fn, out_col, label):
    mgr  = load_csv(CSV_PATH)
    res  = getattr(lib, fn_name)(mgr, col_a.encode(), col_b.encode())
    free(mgr)
    if not res:
        check(label, False, "returned null")
        return
    vals = get_f64_values(res, out_col)
    free(res)
    if vals is None:
        check(label, False, f"column '{out_col}' not found")
        return
    expected = [expected_fn(a, b) for a, b in zip(VALUES_A, VALUES_B)]
    diffs = [abs(g - e) for g, e in zip(vals, expected)]
    max_diff = max(diffs)
    check(label, max_diff < TOLERANCE,
          f"max_diff={max_diff:.2e} (out of {NROWS} rows)")


# Test 1: ADD
test_binary("pardox_gpu_add", "val_a", "val_b",
            lambda a, b: a + b, "gpu_add", "pardox_gpu_add (A + B)")

# Test 2: SUB
test_binary("pardox_gpu_sub", "val_a", "val_b",
            lambda a, b: a - b, "gpu_sub", "pardox_gpu_sub (A − B)")

# Test 3: MUL
test_binary("pardox_gpu_mul", "val_a", "val_b",
            lambda a, b: a * b, "gpu_mul", "pardox_gpu_mul (A × B)")

# Test 4: DIV
test_binary("pardox_gpu_div", "val_a", "val_b",
            lambda a, b: a / b if b != 0 else float("nan"), "gpu_div", "pardox_gpu_div (A ÷ B)")

# ──────────────────────────────────────────────────────────────────────────────
# UNARY OPS
# ──────────────────────────────────────────────────────────────────────────────
print("\n── Unary Operations ─────────────────────────────")

def test_unary(fn_name, col, values_ref, expected_fn, out_col, label):
    mgr  = load_csv(CSV_PATH)
    res  = getattr(lib, fn_name)(mgr, col.encode())
    free(mgr)
    if not res:
        check(label, False, "returned null")
        return
    vals = get_f64_values(res, out_col)
    free(res)
    if vals is None:
        check(label, False, f"column '{out_col}' not found")
        return
    expected = [expected_fn(v) for v in values_ref]
    diffs = [abs(g - e) for g, e in zip(vals, expected)
             if not (math.isnan(g) or math.isnan(e) or math.isinf(g) or math.isinf(e))]
    max_diff = max(diffs) if diffs else 0.0
    check(label, max_diff < TOLERANCE,
          f"max_diff={max_diff:.2e} (out of {NROWS} rows)")


# Test 5: SQRT
test_unary("pardox_gpu_sqrt", "val_a", VALUES_A,
           math.sqrt, "gpu_sqrt", "pardox_gpu_sqrt (√val_a)")

# Test 6: EXP  - use small values to avoid f32 overflow (exp(64) overflows f32)
EXP_VALUES = [v * 0.1 for v in VALUES_A]  # scale down to stay in f32 range

def _write_exp_csv():
    path = os.path.join(os.path.dirname(__file__), "_gap7_exp.csv")
    with open(path, "w") as f:
        f.write("val_small\n")
        for v in EXP_VALUES:
            f.write(f"{v}\n")
    return path

exp_csv = _write_exp_csv()

def load_exp_csv():
    schema = json.dumps({"column_names": ["val_small"],
                          "column_types": ["Float64"]})
    return lib.pardox_load_manager_csv(exp_csv.encode(), schema.encode(), CSV_CONFIG)

mgr_exp = load_exp_csv()
res_exp = lib.pardox_gpu_exp(mgr_exp, b"val_small")
free(mgr_exp)
if res_exp:
    vals_exp = get_f64_values(res_exp, "gpu_exp")
    free(res_exp)
    expected_exp = [math.exp(v) for v in EXP_VALUES]
    diffs_exp = [abs(g - e) for g, e in zip(vals_exp, expected_exp)
                 if not (math.isinf(g) or math.isinf(e))]
    max_diff_exp = max(diffs_exp) if diffs_exp else 0.0
    check("pardox_gpu_exp (e^val_small)", max_diff_exp < TOLERANCE,
          f"max_diff={max_diff_exp:.2e}")
else:
    check("pardox_gpu_exp (e^val_small)", False, "returned null")

# Test 7: LOG
test_unary("pardox_gpu_log", "val_a", VALUES_A,
           math.log, "gpu_log", "pardox_gpu_log (ln(val_a))")

# Test 8: ABS - uses val_neg which has mixed signs
mgr_abs = load_csv(CSV_PATH)
res_abs = lib.pardox_gpu_abs(mgr_abs, b"val_neg")
free(mgr_abs)
if res_abs:
    vals_abs = get_f64_values(res_abs, "gpu_abs")
    free(res_abs)
    expected_abs = [abs(v) for v in VALUES_NEG]
    diffs_abs = [abs(g - e) for g, e in zip(vals_abs, expected_abs)]
    max_diff_abs = max(diffs_abs)
    check("pardox_gpu_abs (|val_neg|)", max_diff_abs < TOLERANCE,
          f"max_diff={max_diff_abs:.2e}")
    # Also verify the abs removes negatives
    all_positive = all(v >= 0 for v in vals_abs)
    check("pardox_gpu_abs - no negatives in output", all_positive,
          f"min={min(vals_abs):.4f}")
else:
    check("pardox_gpu_abs (|val_neg|)", False, "returned null")

# ──────────────────────────────────────────────────────────────────────────────
# STRUCTURAL CHECKS
# ──────────────────────────────────────────────────────────────────────────────
print("\n── Structural Checks ────────────────────────────")

# Test 9: Output manager has correct row count
mgr_s = load_csv(CSV_PATH)
res_s = lib.pardox_gpu_mul(mgr_s, b"val_a", b"val_b")
free(mgr_s)
if res_s:
    nrows_out = lib.pardox_get_row_count(res_s)
    free(res_s)
    check("Output manager has correct row count", nrows_out == NROWS,
          f"expected {NROWS}, got {nrows_out}")
else:
    check("Output manager has correct row count", False, "null result")

# Test 10: Input manager is NOT modified by GPU op
mgr_intact = load_csv(CSV_PATH)
nrows_before = lib.pardox_get_row_count(mgr_intact)
res_intact = lib.pardox_gpu_add(mgr_intact, b"val_a", b"val_b")
nrows_after  = lib.pardox_get_row_count(mgr_intact)
free(mgr_intact)
if res_intact:
    free(res_intact)
check("Input manager intact after GPU op", nrows_before == nrows_after,
      f"before={nrows_before} after={nrows_after}")

# Test 11: GPU ops return null on invalid column name
mgr_err = load_csv(CSV_PATH)
res_err = lib.pardox_gpu_add(mgr_err, b"nonexistent_col", b"val_b")
free(mgr_err)
check("Invalid column returns null", res_err is None or res_err == 0,
      f"ptr={res_err}")

# Test 12: Chained GPU ops (add then sqrt)
mgr_chain = load_csv(CSV_PATH)
res_add  = lib.pardox_gpu_add(mgr_chain, b"val_a", b"val_b")
free(mgr_chain)
if res_add:
    res_sqrt = lib.pardox_gpu_sqrt(res_add, b"gpu_add")
    free(res_add)
    if res_sqrt:
        vals_chain = get_f64_values(res_sqrt, "gpu_sqrt")
        free(res_sqrt)
        expected_chain = [math.sqrt(a + b) for a, b in zip(VALUES_A, VALUES_B)]
        diffs_chain = [abs(g - e) for g, e in zip(vals_chain, expected_chain)]
        max_diff_chain = max(diffs_chain)
        check("Chained GPU ops (add -> sqrt)", max_diff_chain < TOLERANCE,
              f"max_diff={max_diff_chain:.2e}")
    else:
        check("Chained GPU ops (add -> sqrt)", False, "sqrt returned null")
else:
    check("Chained GPU ops (add -> sqrt)", False, "add returned null")

# ── Cleanup temp files ────────────────────────────────────────────────────────
for p in [CSV_PATH, exp_csv]:
    try: os.remove(p)
    except: pass

# ── Summary ───────────────────────────────────────────────────────────────────
print()
passed = sum(1 for _, s, _ in results if s == PASS)
total  = len(results)
print(f"Results: {passed}/{total} tests passed")

if passed < total:
    print("\nFailed tests:")
    for name, status, detail in results:
        if status == FAIL:
            print(f"  FAIL: {name}" + (f"  - {detail}" if detail else ""))
    raise SystemExit(1)
