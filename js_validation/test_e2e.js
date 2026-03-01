'use strict';
/**
 * PardoX Engine v0.3.1 — End-to-End Node.js Validation
 * =====================================================
 * Tests: CSV ingestion, mul(), GPU sort, Observer (heap-alloc JSON),
 *        PostgreSQL write.
 */

const path  = require('path');
// Resolve pardox SDK from local folder (simulating npm local install)
process.chdir(__dirname);
const pardox = require('./pardox/src/index');
const { executeSql } = pardox;

// ---------------------------------------------------------------------------
// Connection strings (local Docker instances)
// ---------------------------------------------------------------------------
const POSTGRES_CONN = 'postgresql://pardox:pardox123@localhost:5434/pardox_test';

function sep(title) {
    console.log('\n' + '='.repeat(60));
    console.log('  ' + title);
    console.log('='.repeat(60));
}

async function main() {

    // -----------------------------------------------------------------------
    // Task 1: Load 50k CSV
    // -----------------------------------------------------------------------
    sep('Task 1: Load hardcore_sales_50k.csv');
    const df = pardox.read_csv('./hardcore_sales_50k.csv');
    const rows = df.shape[0];
    console.log(`  Rows loaded : ${rows.toLocaleString()}`);
    console.log(`  Columns     : ${df.columns.join(', ')}`);
    if (rows !== 50000) throw new Error(`Expected 50000 rows, got ${rows}`);
    console.log('  [PASS] CSV loaded successfully.');

    // -----------------------------------------------------------------------
    // Task 2: Math — total_tax = tax * quantity
    // -----------------------------------------------------------------------
    sep('Task 2: mul(tax, quantity)');
    // quantity is inferred as Int64; pardox_series_mul requires Float64 — cast first
    df.cast('quantity', 'Float64');
    const taxDf = df.mul('tax', 'quantity');
    console.log(`  taxDf columns : ${taxDf.columns.join(', ')}`);
    if (!taxDf.columns.includes('result_mul'))
        throw new Error("Expected column 'result_mul'");
    const taxStd = taxDf.std('result_mul');
    console.log(`  std(total_tax) : ${taxStd.toFixed(4)}`);
    console.log('  [PASS] Multiplication computed.');

    // -----------------------------------------------------------------------
    // Task 3: GPU Sort — sort by latitude ascending
    // -----------------------------------------------------------------------
    sep('Task 3: GPU Sort — sortValues(latitude, ascending=true, gpu=true)');
    const sortedDf = df.sortValues('latitude', true, true);
    console.log(`  Sorted rows : ${sortedDf.shape[0].toLocaleString()}`);
    if (sortedDf.shape[0] !== 50000)
        throw new Error('Row count changed after sort');
    console.log('  [PASS] GPU/CPU sort completed.');

    // -----------------------------------------------------------------------
    // Task 4: Observer — verify heap-allocated JSON (memory patch)
    // -----------------------------------------------------------------------
    sep('Task 4: Observer — toJson() heap-allocation memory patch');
    const jsonStr = sortedDf.toJson();
    const parsed  = JSON.parse(jsonStr);
    const first2  = parsed.slice(0, 2);
    console.log(`  Total records in JSON : ${parsed.length.toLocaleString()}`);
    console.log(`  First record keys     : ${Object.keys(first2[0]).slice(0, 5).join(', ')}`);
    console.log(`  First record latitude : ${first2[0].latitude}`);
    console.log(`  Second record latitude: ${first2[1].latitude}`);
    if (parsed.length !== 50000)
        throw new Error(`Expected 50000 records, got ${parsed.length}`);
    console.log('  [PASS] Observer heap-allocation memory patch working correctly.');

    // -----------------------------------------------------------------------
    // Task 5: Relational Conqueror — PostgreSQL (append)
    // -----------------------------------------------------------------------
    sep('Task 5: Write to PostgreSQL (mode=append)');
    try {
        // execute_sql is single-statement; split DROP and CREATE into separate calls
        executeSql(POSTGRES_CONN, 'DROP TABLE IF EXISTS sales_validation_js');
        executeSql(POSTGRES_CONN, (
            'CREATE TABLE sales_validation_js (' +
            'transaction_id TEXT, timestamp TEXT, date_only TEXT,' +
            'customer_id BIGINT, state TEXT, latitude DOUBLE PRECISION,' +
            'longitude DOUBLE PRECISION, category TEXT, price DOUBLE PRECISION,' +
            'quantity DOUBLE PRECISION, discount DOUBLE PRECISION,' +
            'tax DOUBLE PRECISION, is_refunded TEXT, notes TEXT)'
        ));
        const rowsPg = sortedDf.toSql(POSTGRES_CONN, 'sales_validation_js', 'append');
        console.log(`  Rows written to Postgres : ${rowsPg.toLocaleString()}`);
        if (rowsPg !== 50000)
            throw new Error(`Expected 50000, got ${rowsPg}`);
        console.log('  [PASS] PostgreSQL write successful.');
    } catch (e) {
        console.error(`  [FAIL] PostgreSQL: ${e.message}`);
        process.exit(1);
    }

    // -----------------------------------------------------------------------
    // Summary
    // -----------------------------------------------------------------------
    sep('VALIDATION COMPLETE');
    console.log('  Node.js SDK v0.3.1 — All tasks passed.');
    console.log('  CSV rows processed : 50,000');
    console.log('  Databases written  : PostgreSQL');
}

main().catch(err => {
    console.error('[FATAL]', err);
    process.exit(1);
});
