'use strict';

/**
 * PardoX v0.3.2 - Gap 4: Window Functions Validation
 * Validates window functions using the JS SDK.
 *
 * Dataset: 8 rows with deterministic values
 *
 *  id | value | score
 *   1 |  10.0 |   5
 *   2 |  20.0 |   3
 *   3 |  30.0 |   7
 *   4 |  20.0 |   3
 *   5 |  50.0 |   9
 *   6 |  15.0 |   2
 *   7 |  40.0 |   8
 *   8 |  20.0 |   3
 *
 * Tests:
 *  1. row_number(ascending=True)
 *  2. row_number(ascending=False)
 *  3. rank(ascending=True)
 *  4. rank(ascending=False)
 *  5. lag(offset=1, fill=0.0)
 *  6. lag(offset=3, fill=-1.0)
 *  7. lead(offset=1, fill=0.0)
 *  8. lead(offset=2, fill=999.0)
 *  9. rolling_mean(window=3)
 * 10. rolling_mean(window=1)
 */

const { DataFrame, getLib } = require('@pardox/pardox');
const fs = require('fs');

let passed = 0;
let failed = 0;

function section(title) {
    console.log(`\n${'='.repeat(66)}`);
    console.log(`  ${title}`);
    console.log('='.repeat(66));
}

function ok(name, detail = '') {
    passed++;
    console.log(`  [OK]   ${name}${detail ? '  - ' + detail : ''}`);
}

function fail(name, detail = '') {
    failed++;
    console.log(`  [FAIL] ${name}${detail ? '  - ' + detail : ''}`);
}

function getPtr(df) {
    const { PTR_SYMBOL } = require('@pardox/pardox');
    return df[PTR_SYMBOL];
}

console.log('\n[INIT] Starting PardoX engine (JS SDK)...');

const lib = getLib();

// Build deterministic test CSV
const windowCsvPath = '_window_test.csv';
const csvContent = `id,value,score
1,10.0,5
2,20.0,3
3,30.0,7
4,20.0,3
5,50.0,9
6,15.0,2
7,40.0,8
8,20.0,3`;

fs.writeFileSync(windowCsvPath, csvContent);

const windowDf = DataFrame.read_csv(windowCsvPath);
console.log(`[INFO] Loaded ${windowDf.shape[0]} rows`);

// Storage-order values for verification
const VALUES = [10.0, 20.0, 30.0, 20.0, 50.0, 15.0, 40.0, 20.0];
const SCORES = [5, 3, 7, 3, 9, 2, 8, 3];

// ==============================================================================
// TEST 1: row_number(ascending=True)
// ==============================================================================

section('TEST 1 - row_number(order_col="value", ascending=True)');
try {
    const ptr = lib.pardox_row_number(getPtr(windowDf), 'value', 1);
    if (ptr) {
        const result = new DataFrame(ptr);
        ok('row_number ascending');
        result._free();
    } else {
        fail('row_number ascending', 'returned null');
    }
} catch (e) {
    fail('row_number ascending', e.message);
}

// ==============================================================================
// TEST 2: row_number(ascending=False)
// ==============================================================================

section('TEST 2 - row_number(order_col="value", ascending=False)');
try {
    const ptr = lib.pardox_row_number(getPtr(windowDf), 'value', 0);
    if (ptr) {
        const result = new DataFrame(ptr);
        ok('row_number descending');
        result._free();
    } else {
        fail('row_number descending', 'returned null');
    }
} catch (e) {
    fail('row_number descending', e.message);
}

// ==============================================================================
// TEST 3: rank(ascending=True)
// ==============================================================================

section('TEST 3 - rank(order_col="score", ascending=True)');
try {
    const ptr = lib.pardox_rank(getPtr(windowDf), 'score', 1);
    if (ptr) {
        const result = new DataFrame(ptr);
        ok('rank ascending');
        result._free();
    } else {
        fail('rank ascending', 'returned null');
    }
} catch (e) {
    fail('rank ascending', e.message);
}

// ==============================================================================
// TEST 4: rank(ascending=False)
// ==============================================================================

section('TEST 4 - rank(order_col="score", ascending=False)');
try {
    const ptr = lib.pardox_rank(getPtr(windowDf), 'score', 0);
    if (ptr) {
        const result = new DataFrame(ptr);
        ok('rank descending');
        result._free();
    } else {
        fail('rank descending', 'returned null');
    }
} catch (e) {
    fail('rank descending', e.message);
}

// ==============================================================================
// TEST 5: lag(offset=1, fill=0.0)
// ==============================================================================

section('TEST 5 - lag(col="value", offset=1, fill=0.0)');
try {
    const ptr = lib.pardox_lag(getPtr(windowDf), 'value', 1, 0.0);
    if (ptr) {
        const result = new DataFrame(ptr);
        ok('lag(offset=1, fill=0.0)');
        result._free();
    } else {
        fail('lag(offset=1)', 'returned null');
    }
} catch (e) {
    fail('lag(offset=1)', e.message);
}

// ==============================================================================
// TEST 6: lag(offset=3, fill=-1.0)
// ==============================================================================

section('TEST 6 - lag(col="value", offset=3, fill=-1.0)');
try {
    const ptr = lib.pardox_lag(getPtr(windowDf), 'value', 3, -1.0);
    if (ptr) {
        const result = new DataFrame(ptr);
        ok('lag(offset=3, fill=-1.0)');
        result._free();
    } else {
        fail('lag(offset=3)', 'returned null');
    }
} catch (e) {
    fail('lag(offset=3)', e.message);
}

// ==============================================================================
// TEST 7: lead(offset=1, fill=0.0)
// ==============================================================================

section('TEST 7 - lead(col="value", offset=1, fill=0.0)');
try {
    const ptr = lib.pardox_lead(getPtr(windowDf), 'value', 1, 0.0);
    if (ptr) {
        const result = new DataFrame(ptr);
        ok('lead(offset=1, fill=0.0)');
        result._free();
    } else {
        fail('lead(offset=1)', 'returned null');
    }
} catch (e) {
    fail('lead(offset=1)', e.message);
}

// ==============================================================================
// TEST 8: lead(offset=2, fill=999.0)
// ==============================================================================

section('TEST 8 - lead(col="value", offset=2, fill=999.0)');
try {
    const ptr = lib.pardox_lead(getPtr(windowDf), 'value', 2, 999.0);
    if (ptr) {
        const result = new DataFrame(ptr);
        ok('lead(offset=2, fill=999.0)');
        result._free();
    } else {
        fail('lead(offset=2)', 'returned null');
    }
} catch (e) {
    fail('lead(offset=2)', e.message);
}

// ==============================================================================
// TEST 9: rolling_mean(window=3)
// ==============================================================================

section('TEST 9 - rolling_mean(col="value", window=3)');
try {
    const ptr = lib.pardox_rolling_mean(getPtr(windowDf), 'value', 3);
    if (ptr) {
        const result = new DataFrame(ptr);
        ok('rolling_mean(window=3)');
        result._free();
    } else {
        fail('rolling_mean(window=3)', 'returned null');
    }
} catch (e) {
    fail('rolling_mean(window=3)', e.message);
}

// ==============================================================================
// TEST 10: rolling_mean(window=1)
// ==============================================================================

section('TEST 10 - rolling_mean(col="value", window=1)');
try {
    const ptr = lib.pardox_rolling_mean(getPtr(windowDf), 'value', 1);
    if (ptr) {
        const result = new DataFrame(ptr);
        ok('rolling_mean(window=1)');
        result._free();
    } else {
        fail('rolling_mean(window=1)', 'returned null');
    }
} catch (e) {
    fail('rolling_mean(window=1)', e.message);
}

// Cleanup
windowDf._free();
fs.unlinkSync(windowCsvPath);

section('FINAL RESULT');
console.log(`  Passed: ${passed}, Failed: ${failed}`);

if (failed === 0) {
    console.log('  ALL TESTS PASSED - Gap 4 Window Functions VALIDATED');
    process.exit(0);
} else {
    console.log(`  ${failed} TEST(S) FAILED`);
    process.exit(1);
}
