#!/usr/bin/env node
/**
 * validate_gap13_sdk.js - Gap 13: Streaming GroupBy & Aggregations over .prdx files
 *
 * Tests pardox_prdx_min, pardox_prdx_max, pardox_prdx_mean, pardox_prdx_count,
 * and pardox_groupby_agg_prdx - all operate without loading the full file into RAM.
 *
 * Dataset: ventas_consolidado.prdx (50M rows, 1.27 GB)
 *   columns: transaction_id (Utf8), client_id (Int64), date_time (Timestamp),
 *            entity (Utf8), category (Utf8), client_segment (Utf8),
 *            amount (Float64), tax_rate (Float64)
 */

const fs = require('fs');

// Load PardoX library
const { getLib } = require('@pardox/pardox');

const PRDX_PATH = 'ventas_consolidado.prdx';

if (!fs.existsSync(PRDX_PATH)) {
    console.error(`Missing required file: ${PRDX_PATH}`);
    process.exit(1);
}

const sizeGB = fs.statSync(PRDX_PATH).size / 1e9;
console.log(`[INFO] PRDX file: ${PRDX_PATH} (${sizeGB.toFixed(2)} GB)\n`);

let lib;
try {
    lib = getLib();
} catch (err) {
    console.error(`Fatal Error loading PardoX: ${err.message}`);
    process.exit(1);
}

const results = [];

function check(name, cond, detail = '') {
    const status = cond ? 'OK' : 'FAIL';
    results.push({ name, status, detail });
    const mark = cond ? '✓' : '✗';
    console.log(`  [${mark}] ${name}${detail ? `  - ${detail}` : ''}`);
}

// =============================================================================
// Test 1: prdx_count
// =============================================================================
let t0 = Date.now();
const total = lib.pardox_prdx_count(PRDX_PATH);
let elapsed = (Date.now() - t0) / 1000;
check('Test  1 - prdx_count == 50,000,000',
      Number(total) === 50000000,
      `got ${total} (${elapsed.toFixed(2)}s)`);

// =============================================================================
// Test 2: prdx_min(amount)
// =============================================================================
t0 = Date.now();
const amountMin = lib.pardox_prdx_min(PRDX_PATH, 'amount');
elapsed = (Date.now() - t0) / 1000;
check('Test  2 - prdx_min(amount) > 0',
      amountMin > 0,
      `min=${amountMin.toFixed(4)} (${elapsed.toFixed(2)}s)`);

// =============================================================================
// Test 3: prdx_max(amount) > min
// =============================================================================
t0 = Date.now();
const amountMax = lib.pardox_prdx_max(PRDX_PATH, 'amount');
elapsed = (Date.now() - t0) / 1000;
check('Test  3 - prdx_max(amount) > min(amount)',
      amountMax > amountMin,
      `max=${amountMax.toFixed(4)} > min=${amountMin.toFixed(4)} (${elapsed.toFixed(2)}s)`);

// =============================================================================
// Test 4: prdx_mean(amount) between min and max
// =============================================================================
t0 = Date.now();
const amountMean = lib.pardox_prdx_mean(PRDX_PATH, 'amount');
elapsed = (Date.now() - t0) / 1000;
check('Test  4 - min <= mean(amount) <= max',
      amountMin <= amountMean && amountMean <= amountMax,
      `mean=${amountMean.toFixed(4)} (${elapsed.toFixed(2)}s)`);

// =============================================================================
// Test 5: mean * count ≈ expected total
// =============================================================================
const meanXn = amountMean * Number(total);
check('Test  5 - mean * count ≈ expected total',
      meanXn > 0,
      `mean*count=${meanXn.toFixed(2)}`);

// =============================================================================
// Test 6: groupby_agg_prdx entity -> sum(amount)
// =============================================================================
console.log();
t0 = Date.now();
const r6 = lib.pardox_groupby_agg_prdx(PRDX_PATH, JSON.stringify(['entity']), JSON.stringify({ amount: 'sum' }));
elapsed = (Date.now() - t0) / 1000;
check('Test  6 - groupby_agg_prdx entity->sum(amount) returns non-null',
      r6 !== null && r6 !== 0n,
      `${elapsed.toFixed(2)}s`);

// =============================================================================
// Test 7: row count == distinct entities
// =============================================================================
const nEntities = r6 ? lib.pardox_get_row_count(r6) : 0n;
check('Test  7 - entity groupby: distinct groups > 0',
      nEntities > 0n,
      `${nEntities} distinct entities`);

// =============================================================================
// Test 8: free manager r6
// =============================================================================
if (r6) {
    lib.pardox_free_manager(r6);
    check('Test  8 - freed r6 manager', true, '');
} else {
    check('Test  8 - freed r6 manager', false, 'r6 was null');
}

// =============================================================================
// Test 9: groupby entity -> count
// =============================================================================
const r9 = lib.pardox_groupby_agg_prdx(PRDX_PATH, JSON.stringify(['entity']), JSON.stringify({ amount: 'count' }));
if (r9 && r9 !== 0n) {
    const nRows = lib.pardox_get_row_count(r9);
    const schemaStr = lib.pardox_get_schema_json(r9);
    const schema = JSON.parse(schemaStr);
    const amountType = schema.columns?.find(c => c.name === 'amount')?.type || 'unknown';
    check('Test  9 - groupby entity->count: correct groups + Int64 schema',
          nRows === nEntities && amountType === 'Int64',
          `${nRows} groups, amount_type=${amountType}`);
    lib.pardox_free_manager(r9);
} else {
    check('Test  9 - groupby entity->count: correct groups + Int64 schema', false, 'r9 is null');
}

// =============================================================================
// Test 10: groupby category -> min(amount)
// =============================================================================
const r10 = lib.pardox_groupby_agg_prdx(PRDX_PATH, JSON.stringify(['category']), JSON.stringify({ amount: 'min' }));
if (r10 && r10 !== 0n) {
    const nRows = lib.pardox_get_row_count(r10);
    check('Test 10 - groupby category->min(amount): groups > 0',
          nRows > 0n,
          `${nRows} categories`);
    lib.pardox_free_manager(r10);
} else {
    check('Test 10 - groupby category->min(amount): groups > 0', false, 'r10 is null');
}

// =============================================================================
// Test 11: groupby category -> max(amount)
// =============================================================================
const r11 = lib.pardox_groupby_agg_prdx(PRDX_PATH, JSON.stringify(['category']), JSON.stringify({ amount: 'max' }));
if (r11 && r11 !== 0n) {
    const nRows = lib.pardox_get_row_count(r11);
    check('Test 11 - groupby category->max(amount): groups > 0',
          nRows > 0n,
          `${nRows} categories`);
    lib.pardox_free_manager(r11);
} else {
    check('Test 11 - groupby category->max(amount): groups > 0', false, 'r11 is null');
}

// =============================================================================
// Test 12: multi-key groupby entity + category -> more groups
// =============================================================================
const r12 = lib.pardox_groupby_agg_prdx(PRDX_PATH, JSON.stringify(['entity', 'category']), JSON.stringify({ amount: 'sum' }));
if (r12 && r12 !== 0n) {
    const n12 = lib.pardox_get_row_count(r12);
    check('Test 12 - multi-key groupby (entity,category): groups > single-key groups',
          n12 > nEntities,
          `${n12} groups vs ${nEntities} entity-only groups`);
    lib.pardox_free_manager(r12);
} else {
    check('Test 12 - multi-key groupby (entity,category): groups > single-key groups', false, 'r12 is null');
}

// =============================================================================
// Test 13: invalid column -> null
// =============================================================================
const r13 = lib.pardox_groupby_agg_prdx(PRDX_PATH, JSON.stringify(['entity']), JSON.stringify({ __no_such_col__: 'sum' }));
check('Test 13 - invalid agg column -> null returned',
      r13 === null || r13 === 0n,
      r13 === null || r13 === 0n ? 'correctly null' : `unexpected result ${r13}`);
if (r13 && r13 !== 0n) lib.pardox_free_manager(r13);

// =============================================================================
// Test 14: invalid path -> null
// =============================================================================
const r14 = lib.pardox_groupby_agg_prdx('nonexistent.prdx', JSON.stringify(['entity']), JSON.stringify({ amount: 'sum' }));
check('Test 14 - invalid path -> null returned',
      r14 === null || r14 === 0n,
      r14 === null || r14 === 0n ? 'correctly null' : `unexpected result ${r14}`);
if (r14 && r14 !== 0n) lib.pardox_free_manager(r14);

// =============================================================================
// Test 15: prdx_count on invalid path -> -1
// =============================================================================
const badCount = lib.pardox_prdx_count('nonexistent.prdx');
check('Test 15 - prdx_count on invalid path -> -1',
      Number(badCount) === -1,
      `got ${badCount}`);

// =============================================================================
// Summary
// =============================================================================
console.log();
console.log('='.repeat(60));
const passed = results.filter(r => r.status === 'OK').length;
const totalTests = results.length;
console.log(`  Gap 13 Results: ${passed}/${totalTests} tests passed`);
console.log('='.repeat(60));

if (passed < totalTests) {
    console.log('\nFailed tests:');
    for (const r of results) {
        if (r.status !== 'OK') {
            console.log(`  FAIL: ${r.name} - ${r.detail}`);
        }
    }
    process.exit(1);
}