#!/usr/bin/env python3
"""
validate_gap17_sdk.py - Gap 17: WebAssembly Target

Validates WASM module exists and can be loaded.
Using SDK: Uses pardox_wasm.js from libs/Linux/
"""

import os
import sys
import subprocess

# Add SDK to path

passed = 0
failed = 0

HERE = os.path.dirname(os.path.abspath(__file__))
WASM_DIR = os.path.join(HERE, "pardox", "libs", "Linux")
WASM_JS = os.path.join(WASM_DIR, "pardox_wasm.js")
WASM_WASM = os.path.join(WASM_DIR, "pardox_wasm_bg.wasm")


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  [OK]   {name}" + (f"  - {detail}" if detail else ""))
    else:
        failed += 1
        print(f"  [FAIL] {name}" + (f"  - {detail}" if detail else ""))


print("\n=== Gap 17: WebAssembly Target ===\n")

# Test 1: Check WASM files exist
print("--- Test 1: Check WASM files ---")
check("pardox_wasm.js exists", os.path.exists(WASM_JS), f"path={WASM_JS}")
check("pardox_wasm_bg.wasm exists", os.path.exists(WASM_WASM), f"path={WASM_WASM}")

# Test 2: Check JS runtime
print("\n--- Test 2: Check JS runtime ---")
exe, label = None, None
for candidate, name in [("bun", "bun"), ("node", "node")]:
    try:
        r = subprocess.run([candidate, "--version"], capture_output=True, timeout=5)
        if r.returncode == 0:
            exe = candidate
            ver = (r.stdout.decode().strip() or r.stderr.decode().strip())
            label = f"{name} {ver}"
            break
    except:
        pass

check("JS runtime available", exe is not None, label or "none")

if exe and os.path.exists(WASM_JS):
    # Test 3: Try to load WASM module
    print("\n--- Test 3: Load WASM module ---")
    test_code = f"""
const pardox = require('{WASM_JS}');
console.log('WASM module loaded successfully');
console.log('Module keys:', Object.keys(pardox).slice(0, 10).join(', '));
"""
    result = subprocess.run(
        [exe, "-e", test_code],
        capture_output=True,
        timeout=30,
        cwd=HERE
    )
    check("WASM module loads without error", result.returncode == 0,
          result.stdout.decode().strip()[:100] if result.returncode == 0 else result.stderr.decode()[:100])

print(f"\n{'='*66}")
print(f"Gap 17 Results: {passed} passed, {failed} failed")
print(f"{'='*66}")