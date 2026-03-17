'use strict';

/**
 * PardoX v0.3.2 - Gap 1: GroupBy Validation (GPU + CPU)
 * Validates GroupBy aggregation using the JS SDK.
 *
 * Tests:
 *  1. Load CSVs
 *  2. sales.groupby('category') -> sum(price), mean(quantity)
 *  3. sales.groupby('id_customer') -> sum(price), count(id_transaction)
 *  4. customers.groupby('state') -> count(id_customer)
 *  5. customers.groupby(['state', 'city']) -> count(id_customer)
 *  6. sales.groupby('category') -> min(price), max(quantity), std(discount)
 *  7. sales.groupby('category') -> mean(price), std(discount)
 */

const { DataFrame } = require('@pardox/pardox');

const CUSTOMERS_CSV = 'customers.csv';
const SALES_CSV = 'sales.csv';

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

function check(name, condition, detail = '') {
    if (condition) {
        ok(name, detail);
    } else {
        fail(name, detail);
    }
}

function nrows(df) {
    return df.shape[0];
}

console.log('\n[INIT] Starting PardoX engine (JS SDK)...');

// ==============================================================================
// TEST 1: Load CSVs
// ==============================================================================
section('1. Load CSVs - customers.csv (50K rows) and sales.csv (100K rows)');

let customers, sales;

try {
    customers = DataFrame.read_csv(CUSTOMERS_CSV);
    check('Load customers.csv', nrows(customers) === 50000, `${nrows(customers).toLocaleString()} rows`);
} catch (e) {
    fail('Load customers.csv', e.message);
}

try {
    sales = DataFrame.read_csv(SALES_CSV);
    check('Load sales.csv', nrows(sales) === 100000, `${nrows(sales).toLocaleString()} rows`);
} catch (e) {
    fail('Load sales.csv', e.message);
}

// ==============================================================================
// TEST 2: GroupBy category -> sum(price), mean(quantity)
// ==============================================================================
section('2. sales.groupby("category") -> sum(price), mean(quantity)');

try {
    const r2 = sales.groupBy('category', { price: 'sum', quantity: 'mean' });
    check('GroupBy category returns result', nrows(r2) > 0, `${nrows(r2)} groups`);
    check('GroupBy has 8 categories', nrows(r2) === 8, `got ${nrows(r2)}`);
    r2._free();
} catch (e) {
    fail('GroupBy category', e.message);
}

// ==============================================================================
// TEST 3: GroupBy id_customer -> sum(price), count
// ==============================================================================
section('3. sales.groupby("id_customer") -> sum(price), count(id_transaction)');

try {
    const r3 = sales.groupBy('id_customer', { price: 'sum', id_transaction: 'count' });
    check('GroupBy id_customer returns result', nrows(r3) > 0, `${nrows(r3).toLocaleString()} groups`);
    check('GroupBy has many customers', nrows(r3) > 40000, `got ${nrows(r3).toLocaleString()}`);
    r3._free();
} catch (e) {
    fail('GroupBy id_customer', e.message);
}

// ==============================================================================
// TEST 4: GroupBy state -> count
// ==============================================================================
section('4. customers.groupby("state") -> count(id_customer)');

try {
    const r4 = customers.groupBy('state', { id_customer: 'count' });
    check('GroupBy state returns result', nrows(r4) > 0, `${nrows(r4)} groups`);
    check('GroupBy has 12 states', nrows(r4) === 12, `got ${nrows(r4)}`);
    r4._free();
} catch (e) {
    fail('GroupBy state', e.message);
}

// ==============================================================================
// TEST 5: Multi-column GroupBy (state + city)
// ==============================================================================
section('5. customers.groupby(["state", "city"]) -> count(id_customer)');

try {
    const r5 = customers.groupBy(['state', 'city'], { id_customer: 'count' });
    check('Multi-column GroupBy returns result', nrows(r5) > 0, `${nrows(r5)} groups`);
    check('Multi-column GroupBy has groups', nrows(r5) > 12, `got ${nrows(r5)}`);
    r5._free();
} catch (e) {
    fail('Multi-column GroupBy', e.message);
}

// ==============================================================================
// TEST 6: GroupBy with min, max, std
// ==============================================================================
section('6. sales.groupby("category") -> min(price), max(quantity), std(discount)');

try {
    const r6 = sales.groupBy('category', { price: 'min', quantity: 'max', discount: 'std' });
    check('GroupBy min/max/std returns result', nrows(r6) > 0, `${nrows(r6)} groups`);
    check('GroupBy has 8 categories', nrows(r6) === 8, `got ${nrows(r6)}`);
    r6._free();
} catch (e) {
    fail('GroupBy min/max/std', e.message);
}

// ==============================================================================
// TEST 7: GroupBy with mean and std
// ==============================================================================
section('7. sales.groupby("category") -> mean(price), std(discount)');

try {
    const r7 = sales.groupBy('category', { price: 'mean', discount: 'std' });
    check('GroupBy mean/std returns result', nrows(r7) > 0, `${nrows(r7)} groups`);
    check('GroupBy has 8 categories', nrows(r7) === 8, `got ${nrows(r7)}`);
    r7._free();
} catch (e) {
    fail('GroupBy mean/std', e.message);
}

// ==============================================================================
// Cleanup
// ==============================================================================
if (customers) customers._free();
if (sales) sales._free();

// ==============================================================================
// Summary
// ==============================================================================
section('FINAL RESULT');
console.log(`  Passed: ${passed}, Failed: ${failed}`);

if (failed === 0) {
    console.log('  ALL TESTS PASSED - Gap 1 GroupBy VALIDATED ✓');
    process.exit(0);
} else {
    console.log(`  ${failed} TEST(S) FAILED`);
    process.exit(1);
}
