'use strict';

/**
 * PardoX v0.3.2 - Gap 7: GPU Compute Beyond Sort
 * Validates GPU-accelerated math functions via the JavaScript SDK.
 *
 * Tests:
 *  1. gpuAdd - column addition (A + B)
 *  2. gpuSub - column subtraction (A - B)
 *  3. gpuMul - column multiplication (A * B)
 *  4. gpuDiv - column division (A / B)
 *  5. gpuSqrt - square root
 *  6. gpuExp - exponential (e^x)
 *  7. gpuLog - natural logarithm
 *  8. gpuAbs - absolute value
 *  9. Output manager has correct row count
 * 10. Input manager intact after GPU op
 * 11. Invalid column returns null
 * 12. Chained GPU ops (add -> sqrt)
 */

const { DataFrame, getLib } = require('@pardox/pardox');
const fs = require('fs');

// -- Test data --
// 16 rows; all values positive (for log/sqrt), some will be negated for abs test
const VALUES_A = [1.0, 4.0, 9.0, 16.0, 25.0, 0.5, 2.0, 8.0,
                  0.25, 100.0, 3.0, 7.0, 0.1, 50.0, 0.01, 64.0];
const VALUES_B = [2.0, 2.0, 3.0, 4.0, 5.0, 0.5, 1.0, 2.0,
                  0.5, 10.0, 3.0, 7.0, 0.1, 5.0, 0.01, 8.0];
// neg_val: some negative entries for abs test
const VALUES_NEG = VALUES_A.map((v, i) => i % 2 === 0 ? -v : v);

const NROWS = VALUES_A.length;
const TOLERANCE = 1e-3;   // f32 GPU precision

// -- Write test CSV --
const CSV_PATH = '_gap7_test.csv';
let csvContent = 'val_a,val_b,val_neg\n';
for (let i = 0; i < NROWS; i++) {
    csvContent += `${VALUES_A[i]},${VALUES_B[i]},${VALUES_NEG[i]}\n`;
}
fs.writeFileSync(CSV_PATH, csvContent);

let passed = 0;
let failed = 0;
const results = [];

function section(title) {
    console.log(`\n${'='.repeat(66)}`);
    console.log(`  ${title}`);
    console.log('='.repeat(66));
}

function check(name, condition, detail = '') {
    const status = condition ? 'PASS' : 'FAIL';
    results.push({ name, status, detail });
    const mark = condition ? '[OK]' : '[FAIL]';
    console.log(`  ${mark} ${name}${detail ? '  - ' + detail : ''}`);
    if (condition) passed++;
    else failed++;
}

console.log('\n=== Gap 7: GPU Compute Beyond Sort (JS SDK) ===\n');
console.log(`Test rows: ${NROWS} | Tolerance: ${TOLERANCE}\n`);

const lib = getLib();

// Load CSV using DataFrame
const df = DataFrame.read_csv(CSV_PATH);
const PTR_SYMBOL = Object.getOwnPropertySymbols(df).find(s => s.description === 'pardox.ptr');
const mgrPtr = df[PTR_SYMBOL];

// ===========================================================================
// BINARY OPS
// ===========================================================================
section('Binary Operations');

// Test 1: ADD
try {
    const resDf = df.gpuAdd('val_a', 'val_b');
    const json = resDf.toDict();
    const vals = json.map(row => row.gpu_add);
    const expected = VALUES_A.map((a, i) => a + VALUES_B[i]);
    const diffs = vals.map((g, i) => Math.abs(g - expected[i]));
    const maxDiff = Math.max(...diffs);
    check('gpuAdd (A + B)', maxDiff < TOLERANCE, `max_diff=${maxDiff.toExponential(2)}`);
} catch (e) {
    check('gpuAdd (A + B)', false, e.message);
}

// Test 2: SUB
try {
    const resDf = df.gpuSub('val_a', 'val_b');
    const json = resDf.toDict();
    const vals = json.map(row => row.gpu_sub);
    const expected = VALUES_A.map((a, i) => a - VALUES_B[i]);
    const diffs = vals.map((g, i) => Math.abs(g - expected[i]));
    const maxDiff = Math.max(...diffs);
    check('gpuSub (A - B)', maxDiff < TOLERANCE, `max_diff=${maxDiff.toExponential(2)}`);
} catch (e) {
    check('gpuSub (A - B)', false, e.message);
}

// Test 3: MUL
try {
    const resDf = df.gpuMul('val_a', 'val_b');
    const json = resDf.toDict();
    const vals = json.map(row => row.gpu_mul);
    const expected = VALUES_A.map((a, i) => a * VALUES_B[i]);
    const diffs = vals.map((g, i) => Math.abs(g - expected[i]));
    const maxDiff = Math.max(...diffs);
    check('gpuMul (A * B)', maxDiff < TOLERANCE, `max_diff=${maxDiff.toExponential(2)}`);
} catch (e) {
    check('gpuMul (A * B)', false, e.message);
}

// Test 4: DIV
try {
    const resDf = df.gpuDiv('val_a', 'val_b');
    const json = resDf.toDict();
    const vals = json.map(row => row.gpu_div);
    const expected = VALUES_A.map((a, i) => VALUES_B[i] !== 0 ? a / VALUES_B[i] : NaN);
    const diffs = vals.map((g, i) => Math.abs(g - expected[i]));
    const maxDiff = Math.max(...diffs.filter(d => !isNaN(d)));
    check('gpuDiv (A / B)', maxDiff < TOLERANCE, `max_diff=${maxDiff.toExponential(2)}`);
} catch (e) {
    check('gpuDiv (A / B)', false, e.message);
}

// ===========================================================================
// UNARY OPS
// ===========================================================================
section('Unary Operations');

// Test 5: SQRT
try {
    const resDf = df.gpuSqrt('val_a');
    const json = resDf.toDict();
    const vals = json.map(row => row.gpu_sqrt);
    const expected = VALUES_A.map(Math.sqrt);
    const diffs = vals.map((g, i) => Math.abs(g - expected[i]));
    const maxDiff = Math.max(...diffs);
    check('gpuSqrt (sqrt(val_a))', maxDiff < TOLERANCE, `max_diff=${maxDiff.toExponential(2)}`);
} catch (e) {
    check('gpuSqrt (sqrt(val_a))', false, e.message);
}

// Test 6: EXP - use small values to avoid f32 overflow
const EXP_VALUES = VALUES_A.map(v => v * 0.1);
const EXP_CSV_PATH = '_gap7_exp.csv';
let expCsvContent = 'val_small\n';
for (const v of EXP_VALUES) {
    expCsvContent += `${v}\n`;
}
fs.writeFileSync(EXP_CSV_PATH, expCsvContent);

try {
    const dfExp = DataFrame.read_csv(EXP_CSV_PATH);
    const resDf = dfExp.gpuExp('val_small');
    const json = resDf.toDict();
    const vals = json.map(row => row.gpu_exp);
    const expected = EXP_VALUES.map(Math.exp);
    const diffs = vals.map((g, i) => Math.abs(g - expected[i]));
    const maxDiff = Math.max(...diffs);
    check('gpuExp (e^val_small)', maxDiff < TOLERANCE, `max_diff=${maxDiff.toExponential(2)}`);
} catch (e) {
    check('gpuExp (e^val_small)', false, e.message);
}

// Test 7: LOG
try {
    const resDf = df.gpuLog('val_a');
    const json = resDf.toDict();
    const vals = json.map(row => row.gpu_log);
    const expected = VALUES_A.map(Math.log);
    const diffs = vals.map((g, i) => Math.abs(g - expected[i]));
    const maxDiff = Math.max(...diffs);
    check('gpuLog (ln(val_a))', maxDiff < TOLERANCE, `max_diff=${maxDiff.toExponential(2)}`);
} catch (e) {
    check('gpuLog (ln(val_a))', false, e.message);
}

// Test 8: ABS
try {
    const resDf = df.gpuAbs('val_neg');
    const json = resDf.toDict();
    const vals = json.map(row => row.gpu_abs);
    const expected = VALUES_NEG.map(Math.abs);
    const diffs = vals.map((g, i) => Math.abs(g - expected[i]));
    const maxDiff = Math.max(...diffs);
    const allPositive = vals.every(v => v >= 0);
    check('gpuAbs (|val_neg|)', maxDiff < TOLERANCE && allPositive, `max_diff=${maxDiff.toExponential(2)}`);
} catch (e) {
    check('gpuAbs (|val_neg|)', false, e.message);
}

// ===========================================================================
// STRUCTURAL CHECKS
// ===========================================================================
section('Structural Checks');

// Test 9: Output manager has correct row count
try {
    const resDf = df.gpuMul('val_a', 'val_b');
    const nrows = resDf.shape[0];
    check('Output has correct row count', nrows === NROWS, `expected ${NROWS}, got ${nrows}`);
} catch (e) {
    check('Output has correct row count', false, e.message);
}

// Test 10: Input manager intact after GPU op
const dfIntact = DataFrame.read_csv(CSV_PATH);
const nrowsBefore = dfIntact.shape[0];
try {
    const resDf = dfIntact.gpuAdd('val_a', 'val_b');
    const nrowsAfter = dfIntact.shape[0];
    check('Input intact after GPU op', nrowsBefore === nrowsAfter, `before=${nrowsBefore} after=${nrowsAfter}`);
} catch (e) {
    check('Input intact after GPU op', false, e.message);
}

// Test 11: Invalid column returns null / throws error
let nullResult = false;
try {
    const dfErr = DataFrame.read_csv(CSV_PATH);
    dfErr.gpuAdd('nonexistent_col', 'val_b');
} catch (e) {
    nullResult = true;
}
check('Invalid column throws error', nullResult, 'error thrown as expected');

// Test 12: Chained GPU ops (add -> sqrt)
try {
    const dfChain = DataFrame.read_csv(CSV_PATH);
    const resAdd = dfChain.gpuAdd('val_a', 'val_b');
    const resSqrt = resAdd.gpuSqrt('gpu_add');
    const json = resSqrt.toDict();
    const vals = json.map(row => row.gpu_sqrt);
    const expected = VALUES_A.map((a, i) => Math.sqrt(a + VALUES_B[i]));
    const diffs = vals.map((g, i) => Math.abs(g - expected[i]));
    const maxDiff = Math.max(...diffs);
    check('Chained GPU ops (add -> sqrt)', maxDiff < TOLERANCE, `max_diff=${maxDiff.toExponential(2)}`);
} catch (e) {
    check('Chained GPU ops (add -> sqrt)', false, e.message);
}

// -- Cleanup temp files --
try { fs.unlinkSync(CSV_PATH); } catch (e) {}
try { fs.unlinkSync(EXP_CSV_PATH); } catch (e) {}

// ===========================================================================
// Summary
// ===========================================================================
section('FINAL RESULT');
console.log(`  Passed: ${passed}, Failed: ${failed}`);

if (failed === 0) {
    console.log('  ALL TESTS PASSED - Gap 7 GPU Compute VALIDATED');
    process.exit(0);
} else {
    console.log(`  ${failed} TEST(S) FAILED`);
    process.exit(1);
}
