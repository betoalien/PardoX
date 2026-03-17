'use strict';

/**
 * PardoX v0.3.2 - Gap 9: Time Series Fill
 * Validates ffill, bfill, and interpolate operations via the JavaScript SDK.
 *
 * Tests:
 *  1. ffill - forward fill single null
 *  2. ffill - forward fill multiple consecutive nulls
 *  3. ffill - forward fill at start (null first value)
 *  4. ffill - preserves non-null values
 *  5. bfill - backward fill single null
 *  6. bfill - backward fill multiple consecutive nulls
 *  7. bfill - backward fill at end (null last value)
 *  8. bfill - preserves non-null values
 *  9. interpolate - linear interpolation single null
 * 10. interpolate - linear interpolation multiple nulls
 * 11. interpolate - preserves non-null values
 * 12. interpolate - edge values unchanged
 * 13. ffill - returns new DataFrame
 * 14. bfill - returns new DataFrame
 * 15. interpolate - returns new DataFrame
 * 16. ffill - result has same row count
 * 17. bfill - result has same row count
 * 18. interpolate - result has same row count
 * 19. ffill - all nulls filled except trailing
 * 20. bfill - all nulls filled except leading
 * 21. interpolate - fills all internal nulls
 * 22. ffill - multiple columns
 * 23. bfill - multiple columns
 * 24. interpolate - column with no nulls unchanged
 * 25. ffill - negative values handled
 * 26. bfill - negative values handled
 * 27. interpolate - negative values handled
 * 28. ffill - float values precision
 * 29. bfill - float values precision
 * 30. interpolate - correct interpolation math
 * 31. ffill - invalid column throws
 * 32. bfill - invalid column throws
 * 33. interpolate - invalid column throws
 */

const { DataFrame } = require('@pardox/pardox');
const fs = require('fs');

// -- Create test data with nulls --
// CSV with gaps in the value column
const TIMESERIES_CSV = '_timeseries_test.csv';
const tsData = `date,value
2024-01-01,10.0
2024-01-02,
2024-01-03,30.0
2024-01-04,40.0
2024-01-05,
2024-01-06,
2024-01-07,70.0
2024-01-08,80.0
2024-01-09,
2024-01-10,100.0`;
fs.writeFileSync(TIMESERIES_CSV, tsData);

// -- Test data for multiple columns --
const MULTI_CSV = '_multi_test.csv';
const multiData = `id,val_a,val_b
1,10.0,
2,,20.0
3,30.0,
4,,40.0
5,50.0,50.0`;
fs.writeFileSync(MULTI_CSV, multiData);

// -- Test data for edge cases --
// Leading null for ffill test (cannot fill)
const LEADING_NULL_CSV = '_leading_test.csv';
fs.writeFileSync(LEADING_NULL_CSV, `date,value
2024-01-01,
2024-01-02,20.0
2024-01-03,30.0`);

// Trailing null for bfill test (cannot fill)
const TRAILING_NULL_CSV = '_trailing_test.csv';
fs.writeFileSync(TRAILING_NULL_CSV, `date,value
2024-01-01,10.0
2024-01-02,20.0
2024-01-03,`);

// Negative values
const NEGATIVE_CSV = '_negative_test.csv';
fs.writeFileSync(NEGATIVE_CSV, `date,value
2024-01-01,-10.0
2024-01-02,
2024-01-03,30.0
2024-01-04,-40.0`);

// No nulls (unchanged test)
const NO_NULL_CSV = '_no_null_test.csv';
fs.writeFileSync(NO_NULL_CSV, `date,value
2024-01-01,10.0
2024-01-02,20.0
2024-01-03,30.0`);

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

console.log('\n=== Gap 9: Time Series Fill (JS SDK) ===\n');

const tsDf = DataFrame.read_csv(TIMESERIES_CSV);

// ===========================================================================
// FFILL TESTS
// ===========================================================================
section('FFill Tests');

// Test 1: ffill single null
try {
    // Use a simple dataset for single null test
    const singleCsv = '_single_ffill.csv';
    fs.writeFileSync(singleCsv, 'id,val\n1,10.0\n2,\n3,30.0');
    const dfSingle = DataFrame.read_csv(singleCsv);
    const filled = dfSingle.ffill('val');
    const json = filled.toDict();
    // Row 2 (index 1) should have value 10.0 (forward filled)
    const filledValue = json[1].val;
    check('ffill single null', filledValue === 10.0, `filled value = ${filledValue}`);
    fs.unlinkSync(singleCsv);
} catch (e) {
    check('ffill single null', false, e.message);
}

// Test 2: ffill multiple consecutive nulls
try {
    // Rows 4,5,6 have nulls - should fill with 40.0
    const filled = tsDf.ffill('value');
    const json = filled.toDict();
    // Row at index 4 (2024-01-05) should be 40.0
    const filledValue = json[4].value;
    check('ffill multiple consecutive nulls', filledValue === 40.0, `filled value = ${filledValue}`);
} catch (e) {
    check('ffill multiple consecutive nulls', false, e.message);
}

// Test 3: ffill at start (null first value - cannot fill)
try {
    const dfLead = DataFrame.read_csv(LEADING_NULL_CSV);
    const filled = dfLead.ffill('value');
    const json = filled.toDict();
    // First value remains null (or original), second should be 20.0
    // Since first has no previous value, it stays as-is
    const secondValue = json[1].value;
    check('ffill leading null - second filled', secondValue === 20.0, `second value = ${secondValue}`);
} catch (e) {
    check('ffill leading null', false, e.message);
}

// Test 4: ffill preserves non-null values
try {
    const filled = tsDf.ffill('value');
    const json = filled.toDict();
    // Original non-null values should be preserved
    const original10 = json[0].value;
    const original30 = json[2].value;
    const preserved = original10 === 10.0 && original30 === 30.0;
    check('ffill preserves non-null', preserved, `val[0]=${original10}, val[2]=${original30}`);
} catch (e) {
    check('ffill preserves non-null', false, e.message);
}

// Test 13: ffill returns new DataFrame
try {
    const dfOrig = DataFrame.read_csv(TIMESERIES_CSV);
    const filled = dfOrig.ffill('value');
    const isDifferent = filled !== dfOrig;
    check('ffill returns new DataFrame', isDifferent, 'different object reference');
} catch (e) {
    check('ffill returns new DataFrame', false, e.message);
}

// Test 16: ffill result has same row count
try {
    const filled = tsDf.ffill('value');
    const sameRows = filled.shape[0] === tsDf.shape[0];
    check('ffill same row count', sameRows, `original=${tsDf.shape[0]} filled=${filled.shape[0]}`);
} catch (e) {
    check('ffill same row count', false, e.message);
}

// Test 19: ffill all nulls filled except trailing
try {
    // tsDf has nulls at indices 1, 4, 5, 8
    // After ffill: index 1->30, 4->40, 5->40, 8->100
    // All should be filled since there's a non-null after index 8
    const filled = tsDf.ffill('value');
    const json = filled.toDict();
    const allFilled = json.every(r => r.value !== null && r.value !== undefined && !isNaN(r.value));
    check('ffill all filled', allFilled, 'no null values remain');
} catch (e) {
    check('ffill all filled', false, e.message);
}

// Test 22: ffill multiple columns
try {
    const dfMulti = DataFrame.read_csv(MULTI_CSV);
    const filledA = dfMulti.ffill('val_a');
    const filledB = dfMulti.ffill('val_b');
    const jsonA = filledA.toDict();
    const jsonB = filledB.toDict();
    // Check val_a index 1 (null) filled with 10.0
    const valAFilled = jsonA[1].val_a === 10.0;
    // Check val_b index 0 (null) stays null, index 1 filled with 20.0
    const valBFilled = jsonB[1].val_b === 20.0;
    check('ffill multiple columns', valAFilled && valBFilled, 'both columns filled correctly');
} catch (e) {
    check('ffill multiple columns', false, e.message);
}

// Test 25: ffill negative values handled
try {
    const dfNeg = DataFrame.read_csv(NEGATIVE_CSV);
    const filled = dfNeg.ffill('value');
    const json = filled.toDict();
    // Index 1 should be filled with -10.0
    const filledValue = json[1].value;
    check('ffill negative values', filledValue === -10.0, `filled value = ${filledValue}`);
} catch (e) {
    check('ffill negative values', false, e.message);
}

// Test 28: ffill float values precision
try {
    const floatCsv = '_float_ffill.csv';
    fs.writeFileSync(floatCsv, 'id,val\n1,10.123456\n2,\n3,30.987654');
    const dfFloat = DataFrame.read_csv(floatCsv);
    const filled = dfFloat.ffill('val');
    const json = filled.toDict();
    const filledValue = json[1].val;
    const precisionOk = Math.abs(filledValue - 10.123456) < 0.0001;
    check('ffill float precision', precisionOk, `filled = ${filledValue}, expected 10.123456`);
    fs.unlinkSync(floatCsv);
} catch (e) {
    check('ffill float precision', false, e.message);
}

// Test 31: ffill invalid column throws
let ffillError = false;
try {
    const dfErr = DataFrame.read_csv(TIMESERIES_CSV);
    dfErr.ffill('nonexistent_column');
} catch (e) {
    ffillError = true;
}
check('ffill invalid column throws', ffillError, 'error thrown as expected');

// ===========================================================================
// BFILL TESTS
// ===========================================================================
section('BFill Tests');

// Test 5: bfill single null
try {
    const singleCsv = path.join(HERE, '_single_bfill.csv');
    fs.writeFileSync(singleCsv, 'id,val\n1,10.0\n2,\n3,30.0');
    const dfSingle = DataFrame.read_csv(singleCsv);
    const filled = dfSingle.bfill('val');
    const json = filled.toDict();
    // Row 2 (index 1) should have value 30.0 (backward filled)
    const filledValue = json[1].val;
    check('bfill single null', filledValue === 30.0, `filled value = ${filledValue}`);
    fs.unlinkSync(singleCsv);
} catch (e) {
    check('bfill single null', false, e.message);
}

// Test 6: bfill multiple consecutive nulls
try {
    const dfTs = DataFrame.read_csv(TIMESERIES_CSV);
    const filled = dfTs.bfill('value');
    const json = filled.toDict();
    // Row at index 4 should be 70.0 (next non-null)
    const filledValue = json[4].value;
    check('bfill multiple consecutive nulls', filledValue === 70.0, `filled value = ${filledValue}`);
} catch (e) {
    check('bfill multiple consecutive nulls', false, e.message);
}

// Test 7: bfill at end (null last value - cannot fill)
try {
    const dfTrail = DataFrame.read_csv(TRAILING_NULL_CSV);
    const filled = dfTrail.bfill('value');
    const json = filled.toDict();
    // Last value stays null, second value should be 20.0
    const secondValue = json[1].value;
    check('bfill trailing null - second filled', secondValue === 20.0, `second value = ${secondValue}`);
} catch (e) {
    check('bfill trailing null', false, e.message);
}

// Test 8: bfill preserves non-null values
try {
    const filled = tsDf.bfill('value');
    const json = filled.toDict();
    // Original non-null values should be preserved
    const original10 = json[0].value;
    const original30 = json[2].value;
    const preserved = original10 === 10.0 && original30 === 30.0;
    check('bfill preserves non-null', preserved, `val[0]=${original10}, val[2]=${original30}`);
} catch (e) {
    check('bfill preserves non-null', false, e.message);
}

// Test 14: bfill returns new DataFrame
try {
    const dfOrig = DataFrame.read_csv(TIMESERIES_CSV);
    const filled = dfOrig.bfill('value');
    const isDifferent = filled !== dfOrig;
    check('bfill returns new DataFrame', isDifferent, 'different object reference');
} catch (e) {
    check('bfill returns new DataFrame', false, e.message);
}

// Test 17: bfill result has same row count
try {
    const filled = tsDf.bfill('value');
    const sameRows = filled.shape[0] === tsDf.shape[0];
    check('bfill same row count', sameRows, `original=${tsDf.shape[0]} filled=${filled.shape[0]}`);
} catch (e) {
    check('bfill same row count', false, e.message);
}

// Test 20: bfill all nulls filled except leading
try {
    // tsDf has nulls at indices 1, 4, 5, 8
    // After bfill: index 1->30, 4->70, 5->70, 8->100
    const filled = tsDf.bfill('value');
    const json = filled.toDict();
    const allFilled = json.every(r => r.value !== null && r.value !== undefined && !isNaN(r.value));
    check('bfill all filled', allFilled, 'no null values remain');
} catch (e) {
    check('bfill all filled', false, e.message);
}

// Test 23: bfill multiple columns
try {
    const dfMulti = DataFrame.read_csv(MULTI_CSV);
    const filledA = dfMulti.bfill('val_a');
    const filledB = dfMulti.bfill('val_b');
    const jsonA = filledA.toDict();
    const jsonB = filledB.toDict();
    // Check val_a index 1 (null) filled with 30.0 (next value)
    const valAFilled = jsonA[1].val_a === 30.0;
    // Check val_b index 0 (null) filled with 20.0
    const valBFilled = jsonB[0].val_b === 20.0;
    check('bfill multiple columns', valAFilled && valBFilled, 'both columns filled correctly');
} catch (e) {
    check('bfill multiple columns', false, e.message);
}

// Test 26: bfill negative values handled
try {
    const dfNeg = DataFrame.read_csv(NEGATIVE_CSV);
    const filled = dfNeg.bfill('value');
    const json = filled.toDict();
    // Index 1 should be filled with 30.0 (next value)
    const filledValue = json[1].value;
    check('bfill negative values', filledValue === 30.0, `filled value = ${filledValue}`);
} catch (e) {
    check('bfill negative values', false, e.message);
}

// Test 29: bfill float values precision
try {
    const floatCsv = path.join(HERE, '_float_bfill.csv');
    fs.writeFileSync(floatCsv, 'id,val\n1,10.123456\n2,\n3,30.987654');
    const dfFloat = DataFrame.read_csv(floatCsv);
    const filled = dfFloat.bfill('val');
    const json = filled.toDict();
    const filledValue = json[1].val;
    const precisionOk = Math.abs(filledValue - 30.987654) < 0.0001;
    check('bfill float precision', precisionOk, `filled = ${filledValue}, expected 30.987654`);
    fs.unlinkSync(floatCsv);
} catch (e) {
    check('bfill float precision', false, e.message);
}

// Test 32: bfill invalid column throws
let bfillError = false;
try {
    const dfErr = DataFrame.read_csv(TIMESERIES_CSV);
    dfErr.bfill('nonexistent_column');
} catch (e) {
    bfillError = true;
}
check('bfill invalid column throws', bfillError, 'error thrown as expected');

// ===========================================================================
// INTERPOLATE TESTS
// ===========================================================================
section('Interpolate Tests');

// Test 9: interpolate single null
try {
    const singleCsv = path.join(HERE, '_single_interp.csv');
    fs.writeFileSync(singleCsv, 'id,val\n1,10.0\n2,\n3,30.0');
    const dfSingle = DataFrame.read_csv(singleCsv);
    const filled = dfSingle.interpolate('val');
    const json = filled.toDict();
    // Row 2 (index 1) should have value 20.0 (linear interpolation)
    const filledValue = json[1].val;
    check('interpolate single null', filledValue === 20.0, `filled value = ${filledValue}`);
    fs.unlinkSync(singleCsv);
} catch (e) {
    check('interpolate single null', false, e.message);
}

// Test 10: interpolate multiple nulls
try {
    const dfTs = DataFrame.read_csv(TIMESERIES_CSV);
    const filled = dfTs.interpolate('value');
    const json = filled.toDict();
    // Index 4,5 are between 40 and 70
    // Linear: 40 -> 50 -> 60 -> 70 (index 4 should be ~50, index 5 ~60)
    const val4 = json[4].value;
    const val5 = json[5].value;
    const interpolated = val4 >= 45 && val4 <= 55 && val5 >= 55 && val5 <= 65;
    check('interpolate multiple nulls', interpolated, `val[4]=${val4}, val[5]=${val5}`);
} catch (e) {
    check('interpolate multiple nulls', false, e.message);
}

// Test 11: interpolate preserves non-null values
try {
    const filled = tsDf.interpolate('value');
    const json = filled.toDict();
    const original10 = json[0].value;
    const original30 = json[2].value;
    const preserved = original10 === 10.0 && original30 === 30.0;
    check('interpolate preserves non-null', preserved, `val[0]=${original10}, val[2]=${original30}`);
} catch (e) {
    check('interpolate preserves non-null', false, e.message);
}

// Test 12: interpolate edge values unchanged
try {
    const filled = tsDf.interpolate('value');
    const json = filled.toDict();
    // First and last values should remain unchanged
    const firstVal = json[0].value;
    const lastVal = json[9].value;
    const edgesUnchanged = firstVal === 10.0 && lastVal === 100.0;
    check('interpolate edge values unchanged', edgesUnchanged, `first=${firstVal}, last=${lastVal}`);
} catch (e) {
    check('interpolate edge values', false, e.message);
}

// Test 15: interpolate returns new DataFrame
try {
    const dfOrig = DataFrame.read_csv(TIMESERIES_CSV);
    const filled = dfOrig.interpolate('value');
    const isDifferent = filled !== dfOrig;
    check('interpolate returns new DataFrame', isDifferent, 'different object reference');
} catch (e) {
    check('interpolate returns new DataFrame', false, e.message);
}

// Test 18: interpolate result has same row count
try {
    const filled = tsDf.interpolate('value');
    const sameRows = filled.shape[0] === tsDf.shape[0];
    check('interpolate same row count', sameRows, `original=${tsDf.shape[0]} filled=${filled.shape[0]}`);
} catch (e) {
    check('interpolate same row count', false, e.message);
}

// Test 21: interpolate fills all internal nulls
try {
    const filled = tsDf.interpolate('value');
    const json = filled.toDict();
    const allFilled = json.every(r => r.value !== null && r.value !== undefined && !isNaN(r.value));
    check('interpolate all filled', allFilled, 'no null values remain');
} catch (e) {
    check('interpolate all filled', false, e.message);
}

// Test 24: interpolate column with no nulls unchanged
try {
    const dfNoNull = DataFrame.read_csv(NO_NULL_CSV);
    const filled = dfNoNull.interpolate('value');
    const json = filled.toDict();
    const unchanged = json[0].value === 10.0 && json[1].value === 20.0 && json[2].value === 30.0;
    check('interpolate no nulls unchanged', unchanged, 'values remain the same');
} catch (e) {
    check('interpolate no nulls', false, e.message);
}

// Test 27: interpolate negative values handled
try {
    const dfNeg = DataFrame.read_csv(NEGATIVE_CSV);
    const filled = dfNeg.interpolate('value');
    const json = filled.toDict();
    // Index 1 should be interpolated between -10 and 30 = 10
    const filledValue = json[1].value;
    const correctInterp = Math.abs(filledValue - 10.0) < 0.1;
    check('interpolate negative values', correctInterp, `filled value = ${filledValue}, expected 10.0`);
} catch (e) {
    check('interpolate negative values', false, e.message);
}

// Test 30: interpolate correct interpolation math
try {
    // Test exact linear interpolation: 0, _, _, _, 100
    // Should give: 0, 25, 50, 75, 100
    const exactCsv = path.join(HERE, '_exact_interp.csv');
    fs.writeFileSync(exactCsv, 'id,val\n1,0\n2,\n3,\n4,\n5,100');
    const dfExact = DataFrame.read_csv(exactCsv);
    const filled = dfExact.interpolate('val');
    const json = filled.toDict();
    const val2 = json[1].val;
    const val3 = json[2].val;
    const val4 = json[3].val;
    const correct = Math.abs(val2 - 25) < 0.1 && Math.abs(val3 - 50) < 0.1 && Math.abs(val4 - 75) < 0.1;
    check('interpolate correct math', correct, `[0,${val2},${val3},${val4},100]`);
    fs.unlinkSync(exactCsv);
} catch (e) {
    check('interpolate correct math', false, e.message);
}

// Test 33: interpolate invalid column throws
let interpError = false;
try {
    const dfErr = DataFrame.read_csv(TIMESERIES_CSV);
    dfErr.interpolate('nonexistent_column');
} catch (e) {
    interpError = true;
}
check('interpolate invalid column throws', interpError, 'error thrown as expected');

// -- Cleanup temp files --
const filesToClean = [
    TIMESERIES_CSV, MULTI_CSV, LEADING_NULL_CSV, TRAILING_NULL_CSV,
    NEGATIVE_CSV, NO_NULL_CSV
];
filesToClean.forEach(f => { try { fs.unlinkSync(f); } catch (e) {} });

// ===========================================================================
// Summary
// ===========================================================================
section('FINAL RESULT');
console.log(`  Passed: ${passed}, Failed: ${failed}`);

if (failed === 0) {
    console.log('  ALL TESTS PASSED - Gap 9 Time Series Fill VALIDATED');
    process.exit(0);
} else {
    console.log(`  ${failed} TEST(S) FAILED`);
    process.exit(1);
}
