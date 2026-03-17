#!/usr/bin/env node
/**
 * validate_gap14_sdk.js - Gap 14: SQL over in-memory DataFrame
 *
 * Tests pardox_sql_query - runs standard SQL SELECT directly on any
 * in-memory DataFrame, coexisting with all existing DataFrame APIs.
 *
 * Datasets:
 *   customers.csv - 50,000 rows
 *   sales.csv    - 100,000 rows
 */

const fs = require('fs');
const { getLib } = require('@pardox/pardox');
const { DataFrame } = require('@pardox/pardox');

const CSV_SALES = 'sales.csv';
const CSV_CUST = 'customers.csv';

if (!fs.existsSync(CSV_SALES) || !fs.existsSync(CSV_CUST)) {
    console.error(`Missing required files: ${CSV_SALES} or ${CSV_CUST}`);
    process.exit(1);
}

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
// Load datasets
// =============================================================================
console.log('[LOAD] Loading sales.csv (100K rows)...');
const sales = DataFrame.read_csv(CSV_SALES);
check('sales.csv loaded', sales && Number(sales.shape[0]) === 100000, `${sales ? Number(sales.shape[0]) : 0} rows`);

console.log('[LOAD] Loading customers.csv (50K rows)...');
const cust = DataFrame.read_csv(CSV_CUST);
check('customers.csv loaded', cust && Number(cust.shape[0]) === 50000, `${cust ? Number(cust.shape[0]) : 0} rows`);

console.log('\n=== Gap 14: SQL over DataFrames ===\n');

// =============================================================================
// Test 1: SELECT *
// =============================================================================
let t0 = Date.now();
let r1 = sales.sql('SELECT * FROM t');
let n1 = r1 ? Number(r1.shape[0]) : 0;
let elapsed = (Date.now() - t0) / 1000;
check('Test  1 - SELECT * returns all rows', n1 === 100000, `${n1} rows (${elapsed.toFixed(2)}s)`);
if (r1) r1.free();

// =============================================================================
// Test 2: SELECT specific columns
// =============================================================================
const r2 = sales.sql('SELECT id_transaction, price FROM t');
const n2 = r2 ? Number(r2.shape[0]) : 0;
const cols2 = r2 ? r2.columns : [];
check('Test  2 - SELECT id_transaction, price: 2 columns + all rows',
      n2 === 100000 && cols2.includes('id_transaction') && cols2.includes('price'),
      `cols=${cols2.join(',')} rows=${n2}`);
if (r2) r2.free();

// =============================================================================
// Test 3: SELECT with alias
// =============================================================================
const r3 = sales.sql('SELECT price AS revenue FROM t');
const cols3 = r3 ? r3.columns : [];
check('Test  3 - SELECT price AS revenue: column named "revenue"',
      cols3.includes('revenue'), `cols=${cols3.join(',')}`);
if (r3) r3.free();

// =============================================================================
// Test 4: WHERE numeric
// =============================================================================
t0 = Date.now();
const r4 = sales.sql('SELECT * FROM t WHERE price > 900');
elapsed = (Date.now() - t0) / 1000;
const n4 = r4 ? Number(r4.shape[0]) : 0;
check('Test  4 - WHERE price > 900: filtered rows > 0 and < total',
      n4 > 0 && n4 < 100000, `${n4} rows (${elapsed.toFixed(2)}s)`);
if (r4) r4.free();

// =============================================================================
// Test 5: WHERE string equality
// =============================================================================
const r5 = sales.sql("SELECT * FROM t WHERE category = 'Toys'");
const n5 = r5 ? Number(r5.shape[0]) : 0;
check('Test  5 - WHERE category = "Toys": rows > 0', n5 > 0, `${n5} rows`);
if (r5) r5.free();

// =============================================================================
// Test 6: WHERE AND
// =============================================================================
const r6 = sales.sql('SELECT * FROM t WHERE price > 500 AND quantity > 8');
const n6 = r6 ? Number(r6.shape[0]) : 0;
check('Test  6 - WHERE price > 500 AND quantity > 8: rows > 0', n6 > 0, `${n6} rows`);
if (r6) r6.free();

// =============================================================================
// Test 7: WHERE OR
// =============================================================================
const r7 = sales.sql("SELECT * FROM t WHERE category = 'Toys' OR category = 'Books'");
const n7 = r7 ? Number(r7.shape[0]) : 0;
check('Test  7 - WHERE category IN Toys OR Books: rows > 0', n7 > 0, `${n7} rows`);
if (r7) r7.free();

// =============================================================================
// Test 8: WHERE BETWEEN
// =============================================================================
const r8 = sales.sql('SELECT * FROM t WHERE price BETWEEN 100 AND 200');
const n8 = r8 ? Number(r8.shape[0]) : 0;
check('Test  8 - WHERE price BETWEEN 100 AND 200: rows > 0', n8 > 0, `${n8} rows`);
if (r8) r8.free();

// =============================================================================
// Test 9: WHERE LIKE
// =============================================================================
const r9 = sales.sql("SELECT * FROM t WHERE category LIKE 'Home%'");
const n9 = r9 ? Number(r9.shape[0]) : 0;
check('Test  9 - WHERE category LIKE "Home%": rows > 0', n9 > 0, `${n9} rows`);
if (r9) r9.free();

// =============================================================================
// Test 10: WHERE IN
// =============================================================================
const r10 = cust.sql("SELECT * FROM t WHERE state IN ('CA', 'TX', 'NY')");
const n10 = r10 ? Number(r10.shape[0]) : 0;
check('Test 10 - WHERE state IN (CA,TX,NY): rows > 0 and < total', n10 > 0 && n10 < 50000, `${n10} rows`);
if (r10) r10.free();

// =============================================================================
// Test 11: GROUP BY SUM
// =============================================================================
console.log();
t0 = Date.now();
const r11 = sales.sql('SELECT category, SUM(price) AS total FROM t GROUP BY category');
elapsed = (Date.now() - t0) / 1000;
const n11 = r11 ? Number(r11.shape[0]) : 0;
check('Test 11 - GROUP BY category -> SUM(price): groups > 0',
      n11 > 0, `${n11} groups (${elapsed.toFixed(2)}s)`);
if (r11) r11.free();

// =============================================================================
// Test 12: GROUP BY COUNT
// =============================================================================
const r12 = sales.sql('SELECT category, COUNT(price) AS cnt FROM t GROUP BY category');
const n12 = r12 ? Number(r12.shape[0]) : 0;
const schema12 = r12 ? r12.dtypes : {};
const cntType = schema12.cnt || 'unknown';
check('Test 12 - GROUP BY category -> COUNT(price): Int64 result',
      n12 > 0 && cntType === 'Int64', `${n12} groups cnt_type=${cntType}`);
if (r12) r12.free();

// =============================================================================
// Test 13: ORDER BY ASC
// =============================================================================
const r13 = sales.sql('SELECT price FROM t ORDER BY price ASC LIMIT 5');
const min13 = r13 ? r13['price'].min() : 0;
check('Test 13 - ORDER BY price ASC LIMIT 5: min price > 0',
      min13 > 0, `min=${min13.toFixed(2)}`);
if (r13) r13.free();

// =============================================================================
// Test 14: ORDER BY DESC
// =============================================================================
const r14 = sales.sql('SELECT price FROM t ORDER BY price DESC LIMIT 5');
const max14 = r14 ? r14['price'].max() : 0;
check('Test 14 - ORDER BY price DESC LIMIT 5: max price > min',
      max14 > min13, `max=${max14.toFixed(2)} > min=${min13.toFixed(2)}`);
if (r14) r14.free();

// =============================================================================
// Test 15: LIMIT 10
// =============================================================================
const r15 = sales.sql('SELECT * FROM t LIMIT 10');
const n15 = r15 ? Number(r15.shape[0]) : 0;
check('Test 15 - LIMIT 10: exactly 10 rows', n15 === 10, `got ${n15}`);
if (r15) r15.free();

// =============================================================================
// Test 16: SELECT COUNT(*)
// =============================================================================
const r16 = sales.sql('SELECT COUNT(*) AS n FROM t');
const n16 = r16 ? Number(r16.shape[0]) : 0;
const schema16 = r16 ? r16.dtypes : {};
const nType = schema16.n || 'unknown';
check('Test 16 - SELECT COUNT(*): 1 row, Int64 type', n16 === 1 && nType === 'Int64', `rows=${n16} type=${nType}`);
if (r16) r16.free();

// =============================================================================
// Test 17: SELECT SUM(price)
// =============================================================================
const r17 = sales.sql('SELECT SUM(price) AS total FROM t');
const n17 = r17 ? Number(r17.shape[0]) : 0;
const total17 = r17 ? r17['total'].sum() : 0;
check('Test 17 - SELECT SUM(price): 1 row, value > 0',
      n17 === 1 && total17 > 0, `total=${total17.toFixed(2)}`);
if (r17) r17.free();

// =============================================================================
// Test 18: WHERE + ORDER BY + LIMIT
// =============================================================================
t0 = Date.now();
const r18 = sales.sql('SELECT id_transaction, price FROM t WHERE price > 800 ORDER BY price DESC LIMIT 20');
elapsed = (Date.now() - t0) / 1000;
const n18 = r18 ? Number(r18.shape[0]) : 0;
check('Test 18 - WHERE + ORDER BY DESC + LIMIT 20: 20 rows',
      n18 === 20, `(${elapsed.toFixed(2)}s) ${n18} rows`);
if (r18) r18.free();

// =============================================================================
// Test 19: NOT in WHERE
// =============================================================================
const r5_toys = sales.sql("SELECT * FROM t WHERE category = 'Toys'");
const n5_toys = r5_toys ? Number(r5_toys.shape[0]) : 0;
const r19 = sales.sql("SELECT * FROM t WHERE NOT (category = 'Toys')");
const n19 = r19 ? Number(r19.shape[0]) : 0;
check('Test 19 - NOT (category = "Toys"): rows + toys rows == total',
      n19 + n5_toys === 100000, `not_toys=${n19} + toys=${n5_toys} = ${n19 + n5_toys}`);
if (r5_toys) r5_toys.free();
if (r19) r19.free();

// =============================================================================
// Test 20: Invalid SQL -> null
// =============================================================================
const r20 = sales.sql('NOT VALID SQL @@@@');
check('Test 20 - invalid SQL -> null returned', r20 === null, r20 === null ? 'correctly null' : 'unexpected result');

// =============================================================================
// Test 21: Unknown column in WHERE -> null
// =============================================================================
const r21 = sales.sql('SELECT * FROM t WHERE __no_such_col__ > 0');
check('Test 21 - unknown column in WHERE -> null returned',
      r21 === null, r21 === null ? 'correctly null' : 'unexpected result');

// =============================================================================
// Cleanup
// =============================================================================
sales.free();
cust.free();

// =============================================================================
// Summary
// =============================================================================
console.log();
console.log('='.repeat(60));
const passed = results.filter(r => r.status === 'OK').length;
const total = results.length;
console.log(`  Gap 14 Results: ${passed}/${total} tests passed`);
console.log('='.repeat(60));

if (passed < total) {
    console.log('\nFailed tests:');
    for (const r of results) {
        if (r.status !== 'OK') {
            console.log(`  FAIL: ${r.name} - ${r.detail}`);
        }
    }
    process.exit(1);
}