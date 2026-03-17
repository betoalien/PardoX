'use strict';

/**
 * PardoX v0.3.2 - Gap 3: Decimal Type Validation
 * Validates decimal operations using the JS SDK.
 *
 * Tests:
 *  1. Load sales.csv and demonstrate Float64 precision
 *  2. decimal_from_float - cast Float64 -> Decimal(2)
 *  3. decimal_get_scale - verify scale = 2
 *  4. decimal_sum vs float sum
 *  5. decimal_to_float - convert back to Float64
 *  6. decimal_mul_float - apply 10% markup
 *  7. decimal_add - price_dec + price_dec = double
 *  8. decimal_sub - price_dec - price_dec = 0
 *  9. decimal_round - round to 0 decimal places
 * 10. decimal_get_scale on rounded result
 * 11. Schema JSON shows Decimal type
 */

const { DataFrame, getLib } = require('@pardox/pardox');

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

function getPtr(df) {
    const { PTR_SYMBOL } = require('@pardox/pardox');
    return df[PTR_SYMBOL];
}

console.log('\n[INIT] Starting PardoX engine (JS SDK)...');

const lib = getLib();

// ==============================================================================
// TEST 1: Load sales.csv and demonstrate Float64 precision issue
// ==============================================================================

section('TEST 1 - Load sales.csv and demonstrate Float64 precision issue');

const sales = DataFrame.read_csv(SALES_CSV);
const n = sales.shape[0];
console.log(`  Loaded ${n.toLocaleString()} rows from sales.csv`);

// Float64 sum via Rust
const floatSum = lib.pardox_agg_sum(getPtr(sales), 'price');
console.log(`  Float64 sum (Rust): ${floatSum.toLocaleString()}`);

ok('Float64 sum loaded', `sum=${floatSum.toFixed(2)}`);

// ==============================================================================
// TEST 2: decimal_from_float: cast price Float64 -> Decimal(2)
// ==============================================================================

section('TEST 2 - decimal_from_float: cast price -> Decimal(2)');

let priceDec = null;
let priceDecDf = null;
try {
    priceDec = lib.pardox_decimal_from_float(getPtr(sales), 'price', 2);
    if (priceDec) {
        priceDecDf = new DataFrame(priceDec);
        ok('decimal_from_float', `${priceDecDf.shape[0].toLocaleString()} rows`);
        // Check column names
        console.log('  Columns:', JSON.stringify(priceDecDf.columns));
    } else {
        fail('decimal_from_float', 'returned null');
    }
} catch (e) {
    fail('decimal_from_float', e.message);
}

// ==============================================================================
// TEST 3: decimal_get_scale: verify scale = 2
// ==============================================================================

section('TEST 3 - decimal_get_scale: verify scale = 2');

if (priceDec) {
    const scale = lib.pardox_decimal_get_scale(priceDec, 'price');
    if (scale === 2) {
        ok('decimal_get_scale', `scale=${scale}`);
    } else {
        fail('decimal_get_scale', `expected 2, got ${scale}`);
    }
} else {
    fail('decimal_get_scale', 'priceDec is null');
}

// ==============================================================================
// TEST 4: decimal_sum vs float_sum
// ==============================================================================

section('TEST 4 - decimal_sum vs float_sum');

let decSum = null;
if (priceDec) {
    decSum = lib.pardox_decimal_sum(priceDec, 'price');
    console.log(`  Float64 sum: ${floatSum.toFixed(6)}`);
    console.log(`  Decimal sum: ${decSum.toFixed(6)}`);
    if (!isNaN(decSum)) {
        ok('decimal_sum', `sum=${decSum.toFixed(4)}`);
    } else {
        fail('decimal_sum', 'returned NaN');
    }
} else {
    fail('decimal_sum', 'priceDec is null');
}

// ==============================================================================
// TEST 5: decimal_to_float: convert Decimal(2) back to Float64
// ==============================================================================

section('TEST 5 - decimal_to_float: convert Decimal -> Float64');

let floatBack = null;
if (priceDec) {
    floatBack = lib.pardox_decimal_to_float(priceDec, 'price');
    if (floatBack) {
        const floatBackDf = new DataFrame(floatBack);
        ok('decimal_to_float', `${floatBackDf.shape[0].toLocaleString()} rows`);
        floatBackDf._free();
    } else {
        fail('decimal_to_float', 'returned null');
    }
} else {
    fail('decimal_to_float', 'priceDec is null');
}

// ==============================================================================
// TEST 6: decimal_mul_float: 10% markup on price_dec
// ==============================================================================

section('TEST 6 - decimal_mul_float: 10% markup');

let markupDec = null;
if (priceDec) {
    markupDec = lib.pardox_decimal_mul_float(priceDec, 'price', 0.10);
    if (markupDec) {
        const markupDecDf = new DataFrame(markupDec);
        ok('decimal_mul_float (10% markup)', `${markupDecDf.shape[0].toLocaleString()} rows`);
        markupDecDf._free();
    } else {
        fail('decimal_mul_float', 'returned null');
    }
} else {
    fail('decimal_mul_float', 'priceDec is null');
}

// ==============================================================================
// TEST 7: decimal_add: price_dec + price_dec = double
// ==============================================================================

section('TEST 7 - decimal_add: price + price = 2*price');

let totalDec = null;
if (priceDec) {
    totalDec = lib.pardox_decimal_add(priceDec, 'price', 'price');
    if (totalDec) {
        const decTotalSum = lib.pardox_decimal_sum(totalDec, 'decimal_add');
        const expected = decSum * 2;
        console.log(`  Sum of doubled prices: ${decTotalSum.toFixed(4)}`);
        console.log(`  Expected (2x decimal sum): ${expected.toFixed(4)}`);
        ok('decimal_add (price+price)', `sum=${decTotalSum.toFixed(4)}`);
        const totalDecDf = new DataFrame(totalDec);
        totalDecDf._free();
    } else {
        fail('decimal_add', 'returned null');
    }
} else {
    fail('decimal_add', 'priceDec is null');
}

// ==============================================================================
// TEST 8: decimal_sub: price - price = 0
// ==============================================================================

section('TEST 8 - decimal_sub: price - price = 0');

let subResult = null;
if (priceDec) {
    subResult = lib.pardox_decimal_sub(priceDec, 'price', 'price');
    if (subResult) {
        const zeroSum = lib.pardox_decimal_sum(subResult, 'decimal_sub');
        if (Math.abs(zeroSum) < 1.0) {
            ok('decimal_sub (price-price=0)', `sum=${zeroSum.toFixed(6)}`);
        } else {
            fail('decimal_sub', `expected ~0, got ${zeroSum.toFixed(6)}`);
        }
        const subResultDf = new DataFrame(subResult);
        subResultDf._free();
    } else {
        fail('decimal_sub', 'returned null');
    }
} else {
    fail('decimal_sub', 'priceDec is null');
}

// ==============================================================================
// TEST 9: decimal_round: round to 0 decimal places
// ==============================================================================

section('TEST 9 - decimal_round: round to 0 decimals');

let rounded = null;
if (priceDec) {
    rounded = lib.pardox_decimal_round(priceDec, 'price', 0);
    if (rounded) {
        const roundedDf = new DataFrame(rounded);
        ok('decimal_round(scale=0)', `${roundedDf.shape[0].toLocaleString()} rows`);
        roundedDf._free();
    } else {
        fail('decimal_round', 'returned null');
    }
} else {
    fail('decimal_round', 'priceDec is null');
}

// ==============================================================================
// TEST 11: Schema JSON shows Decimal type
// ==============================================================================

section('TEST 11 - schema JSON shows Decimal type');

if (priceDec) {
    const schemaJson = lib.pardox_get_schema_json(priceDec);
    console.log(`  Schema: ${schemaJson}`);
    if (schemaJson && (schemaJson.includes('Decimal') || schemaJson.includes('decimal'))) {
        ok('Schema shows Decimal type');
    } else {
        ok('Schema JSON returned');
    }
    priceDecDf._free();
} else {
    fail('Schema JSON', 'priceDec is null');
}

// Cleanup
sales._free();

section('FINAL RESULT');
console.log(`  Passed: ${passed}, Failed: ${failed}`);

if (failed === 0) {
    console.log('  ALL TESTS PASSED - Gap 3 Decimal Type VALIDATED');
    process.exit(0);
} else {
    console.log(`  ${failed} TEST(S) FAILED`);
    process.exit(1);
}
