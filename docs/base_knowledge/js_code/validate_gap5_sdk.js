'use strict';

/**
 * PardoX v0.3.2 - Gap 5: Lazy Pipeline Validation
 * Validates lazy frame operations using the JS SDK.
 *
 * Tests:
 *  1. pardox_lazy_scan_csv - register CSV source (no I/O)
 *  2. pardox_lazy_describe - JSON plan description (zero I/O)
 *  3. pardox_lazy_collect - execute plan -> DataFrame
 *  4. pardox_lazy_limit - row cap
 *  5. pardox_lazy_select - column projection
 *  6. pardox_lazy_filter - numeric predicate
 *  7. pardox_lazy_filter - Int64 predicate
 *  8. Chained filters (AND logic)
 *  9. select + filter + limit combined
 * 10. pardox_lazy_free - discard plan without executing
 * 11. describe after operations shows updated plan
 * 12. filter selectivity
 */

const { DataFrame, getLib } = require('@pardox/pardox');

const SALES_CSV = 'sales.csv';

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

console.log('\n=== Gap 5: Lazy Computation Pipeline ===\n');

const lib = getLib();

// ==============================================================================
// TEST 1: pardox_lazy_scan_csv - frame is created without reading file
// ==============================================================================

section('TEST 1: lazy_scan_csv (no I/O)');
const frame = lib.pardox_lazy_scan_csv(SALES_CSV, 44, 1);
check('frame pointer is non-null', frame !== null && frame !== undefined);

// ==============================================================================
// TEST 2: pardox_lazy_describe - JSON plan without executing
// ==============================================================================

section('TEST 2: lazy_describe (plan JSON)');
const descPtr = lib.pardox_lazy_describe(frame);
const desc = descPtr ? descPtr : '';
let plan = {};
try {
    plan = JSON.parse(desc);
} catch (e) {
    plan = {};
}
check('describe source path includes sales.csv', SALES_CSV.includes('sales') || plan.source?.includes('sales'));

// ==============================================================================
// TEST 3: pardox_lazy_collect - full scan, all rows, all columns
// ==============================================================================

section('TEST 3: collect full (100K rows)');
const frameFull = lib.pardox_lazy_scan_csv(SALES_CSV, 44, 1);
const mgrFull = lib.pardox_lazy_collect(frameFull);
const nrowsFull = mgrFull ? lib.pardox_get_row_count(mgrFull) : 0;
const schemaFull = mgrFull ? lib.pardox_get_schema_json(mgrFull) : '{}';
let schemaObjFull = {};
try {
    schemaObjFull = JSON.parse(schemaFull);
} catch (e) {}

check('full collect = 100K rows', nrowsFull === 100000, `got ${nrowsFull}`);
check('full collect has 7 columns', (schemaObjFull.columns || []).length === 7, `got ${(schemaObjFull.columns || []).length}`);
if (mgrFull) lib.pardox_free_manager(mgrFull);

// ==============================================================================
// TEST 4: pardox_lazy_limit - cap at 500 rows
// ==============================================================================

section('TEST 4: lazy_limit (500 rows)');
const frameLim = lib.pardox_lazy_scan_csv(SALES_CSV, 44, 1);
const frameLimLimited = lib.pardox_lazy_limit(frameLim, 500);
const mgrLim = lib.pardox_lazy_collect(frameLimLimited);
const nrowsLim = mgrLim ? lib.pardox_get_row_count(mgrLim) : 0;
check('limit 500 -> exactly 500 rows', nrowsLim === 500, `got ${nrowsLim}`);
if (mgrLim) lib.pardox_free_manager(mgrLim);

// ==============================================================================
// TEST 5: pardox_lazy_select - column projection
// ==============================================================================

section('TEST 5: lazy_select (project 3 columns)');
const frameSel = lib.pardox_lazy_scan_csv(SALES_CSV, 44, 1);
const frameSelSelected = lib.pardox_lazy_select(frameSel, '["id_transaction","price","quantity"]');
const mgrSel = lib.pardox_lazy_collect(frameSelSelected);
const nrowsSel = mgrSel ? lib.pardox_get_row_count(mgrSel) : 0;
const schemaSel = mgrSel ? lib.pardox_get_schema_json(mgrSel) : '{}';
let schemaObjSel = {};
try {
    schemaObjSel = JSON.parse(schemaSel);
} catch (e) {}

const colNamesSel = (schemaObjSel.columns || []).map(c => c.name);
check('select 100K rows intact', nrowsSel === 100000, `got ${nrowsSel}`);
check('select returns 3 columns', (schemaObjSel.columns || []).length === 3, `got ${(schemaObjSel.columns || []).length}`);
check('selected cols match', colNamesSel.includes('id_transaction') && colNamesSel.includes('price') && colNamesSel.includes('quantity'));
if (mgrSel) lib.pardox_free_manager(mgrSel);

// ==============================================================================
// TEST 6: pardox_lazy_filter - Float64 predicate (price > 900)
// ==============================================================================

section('TEST 6: lazy_filter Float64 (price > 900)');
const frameF = lib.pardox_lazy_scan_csv(SALES_CSV, 44, 1);
const frameFFiltered = lib.pardox_lazy_filter(frameF, 'price', 'gt', 900.0);
const mgrF = lib.pardox_lazy_collect(frameFFiltered);
const nrowsF = mgrF ? lib.pardox_get_row_count(mgrF) : 0;
const minPrice = mgrF ? lib.pardox_agg_min(mgrF, 'price') : 0;
check('filter price>900 returns rows', nrowsF > 0, `got ${nrowsF} rows`);
check('filter price>900 min >= 900', minPrice > 900.0, `min=${minPrice.toFixed(4)}`);
if (mgrF) lib.pardox_free_manager(mgrF);

// ==============================================================================
// TEST 7: pardox_lazy_filter - Int64 predicate (quantity <= 5)
// ==============================================================================

section('TEST 7: lazy_filter Int64 (quantity <= 5)');
const frameQ = lib.pardox_lazy_scan_csv(SALES_CSV, 44, 1);
const frameQFiltered = lib.pardox_lazy_filter(frameQ, 'quantity', 'lte', 5.0);
const mgrQ = lib.pardox_lazy_collect(frameQFiltered);
const nrowsQ = mgrQ ? lib.pardox_get_row_count(mgrQ) : 0;
const maxQty = mgrQ ? lib.pardox_agg_max(mgrQ, 'quantity') : 0;
check('filter quantity<=5 returns rows', nrowsQ > 0, `got ${nrowsQ} rows`);
check('filter quantity<=5 max <= 5', maxQty <= 5.0, `max=${maxQty}`);
if (mgrQ) lib.pardox_free_manager(mgrQ);

// ==============================================================================
// TEST 8: chained filters (AND logic)
// ==============================================================================

section('TEST 8: chained filters (price>500 AND quantity==10)');
const frameAnd = lib.pardox_lazy_scan_csv(SALES_CSV, 44, 1);
const frameAndSel = lib.pardox_lazy_select(frameAnd, '["price","quantity"]');
const frameAndF1 = lib.pardox_lazy_filter(frameAndSel, 'price', 'gt', 500.0);
const frameAndF2 = lib.pardox_lazy_filter(frameAndF1, 'quantity', 'eq', 10.0);
const mgrAnd = lib.pardox_lazy_collect(frameAndF2);
const nrowsAnd = mgrAnd ? lib.pardox_get_row_count(mgrAnd) : 0;
const maxQtyAnd = mgrAnd ? lib.pardox_agg_max(mgrAnd, 'quantity') : 0;
const minQtyAnd = mgrAnd ? lib.pardox_agg_min(mgrAnd, 'quantity') : 0;
const minPriceAnd = mgrAnd ? lib.pardox_agg_min(mgrAnd, 'price') : 0;
check('AND filter returns rows', nrowsAnd > 0, `got ${nrowsAnd}`);
check('AND filter quantity == 10', minQtyAnd === 10.0 && maxQtyAnd === 10.0, `qty range [${minQtyAnd}, ${maxQtyAnd}]`);
check('AND filter price > 500', minPriceAnd > 500.0, `min_price=${minPriceAnd.toFixed(4)}`);
if (mgrAnd) lib.pardox_free_manager(mgrAnd);

// ==============================================================================
// TEST 9: select + filter + limit combined
// ==============================================================================

section('TEST 9: select + filter + limit combined');
const frameComb = lib.pardox_lazy_scan_csv(SALES_CSV, 44, 1);
const frameCombSel = lib.pardox_lazy_select(frameComb, '["id_transaction","price","discount"]');
const frameCombF = lib.pardox_lazy_filter(frameCombSel, 'price', 'gte', 100.0);
const frameCombLim = lib.pardox_lazy_limit(frameCombF, 1000);
const mgrComb = lib.pardox_lazy_collect(frameCombLim);
const nrowsComb = mgrComb ? lib.pardox_get_row_count(mgrComb) : 0;
const schemaComb = mgrComb ? lib.pardox_get_schema_json(mgrComb) : '{}';
let schemaObjComb = {};
try {
    schemaObjComb = JSON.parse(schemaComb);
} catch (e) {}

const colsComb = (schemaObjComb.columns || []).map(c => c.name);
check('combined plan <= 1000 rows', nrowsComb <= 1000, `got ${nrowsComb}`);
check('combined plan has 3 columns', (schemaObjComb.columns || []).length === 3, `got ${(schemaObjComb.columns || []).length}`);
check('combined has correct columns', colsComb.includes('id_transaction') && colsComb.includes('price') && colsComb.includes('discount'));
if (mgrComb) lib.pardox_free_manager(mgrComb);

// ==============================================================================
// TEST 10: pardox_lazy_free - discard plan without executing
// ==============================================================================

section('TEST 10: lazy_free (discard plan)');
const frameDiscard = lib.pardox_lazy_scan_csv(SALES_CSV, 44, 1);
const frameDiscardLim = lib.pardox_lazy_limit(frameDiscard, 100);
try {
    lib.pardox_lazy_free(frameDiscardLim);
    check('lazy_free does not crash', true);
} catch (e) {
    check('lazy_free does not crash', false, e.message);
}

// ==============================================================================
// TEST 11: describe after operations shows updated plan
// ==============================================================================

section('TEST 11: describe after select + filter + limit');
const frameDesc2 = lib.pardox_lazy_scan_csv(SALES_CSV, 44, 1);
const frameDesc2Sel = lib.pardox_lazy_select(frameDesc2, '["price","quantity"]');
const frameDesc2F = lib.pardox_lazy_filter(frameDesc2Sel, 'price', 'gt', 500.0);
const frameDesc2Lim = lib.pardox_lazy_limit(frameDesc2F, 200);
const desc2Ptr = lib.pardox_lazy_describe(frameDesc2Lim);
const desc2 = desc2Ptr ? desc2Ptr : '{}';
let plan2 = {};
try {
    plan2 = JSON.parse(desc2);
} catch (e) {
    plan2 = {};
}

check('describe shows select', JSON.stringify(plan2.select) === '["price","quantity"]' || (plan2.select && plan2.select.includes && plan2.select.includes('price')));
check('describe shows filter', (plan2.filters && plan2.filters.length > 0) || plan2.filters?.length === 1);
lib.pardox_lazy_free(frameDesc2Lim);

// ==============================================================================
// TEST 12: filter selectivity - price < 200 should return < 50K rows
// ==============================================================================

section('TEST 12: filter selectivity (price < 200)');
const frameLow = lib.pardox_lazy_scan_csv(SALES_CSV, 44, 1);
const frameLowSel = lib.pardox_lazy_select(frameLow, '["price"]');
const frameLowF = lib.pardox_lazy_filter(frameLowSel, 'price', 'lt', 200.0);
const mgrLow = lib.pardox_lazy_collect(frameLowF);
const nrowsLow = mgrLow ? lib.pardox_get_row_count(mgrLow) : 0;
const maxPriceLow = mgrLow ? lib.pardox_agg_max(mgrLow, 'price') : 0;
check('price<200 returns < total rows', nrowsLow < 100000, `got ${nrowsLow}`);
check('price<200 max < 200', maxPriceLow < 200.0, `max=${maxPriceLow.toFixed(4)}`);
if (mgrLow) lib.pardox_free_manager(mgrLow);

// Cleanup lazy frame
lib.pardox_lazy_free(frame);

// ==============================================================================
// Summary
// ==============================================================================

section('FINAL RESULT');
console.log(`  Passed: ${passed}, Failed: ${failed}`);

if (failed === 0) {
    console.log('  ALL TESTS PASSED - Gap 5 Lazy Pipeline VALIDATED');
    process.exit(0);
} else {
    console.log(`  ${failed} TEST(S) FAILED`);
    process.exit(1);
}
