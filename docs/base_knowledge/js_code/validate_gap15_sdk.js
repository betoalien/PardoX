#!/usr/bin/env node
/**
 * validate_gap15_sdk.js - Gap 15: Cloud Storage Native (S3 / GCS / Azure)
 *
 * Tests pardox_cloud_read_csv, pardox_cloud_read_prdx, pardox_cloud_write_prdx
 * Note: Cloud functions may not be available in all builds.
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

// Internal symbol for accessing the raw pointer
const PTR_SYMBOL = Symbol.for('pardox.ptr');

function check(name, cond, detail = '') {
    const status = cond ? 'PASS' : 'FAIL';
    results.push({ name, status, detail });
    const mark = cond ? '✓' : '✗';
    console.log(`  [${mark}] ${name}${detail ? `  - ${detail}` : ''}`);
}

// =============================================================================
// Test: Check if cloud functions exist
// =============================================================================
console.log('\n[TEST] Checking Cloud Storage functions availability...');

const hasCloudReadCsv = typeof lib.pardox_cloud_read_csv === 'function';
const hasCloudWritePrdx = typeof lib.pardox_cloud_write_prdx === 'function';
const hasCloudReadPrdx = typeof lib.pardox_cloud_read_prdx === 'function';

check('pardox_cloud_read_csv exists', hasCloudReadCsv);
check('pardox_cloud_write_prdx exists', hasCloudWritePrdx);
check('pardox_cloud_read_prdx exists', hasCloudReadPrdx);

// =============================================================================
// Test: Basic CSV/PRDX operations (file:// equivalent)
// =============================================================================
console.log('\n[TEST] Basic file operations...');

// Load sales.csv as reference
const salesRef = DataFrame.read_csv(CSV_SALES);
const refRowCount = salesRef ? Number(salesRef.shape[0]) : 0;
check('sales.csv loaded (100K rows)', refRowCount === 100000, `${refRowCount} rows`);

// Get sum of prices
let refSum = 0;
if (salesRef && salesRef[PTR_SYMBOL]) {
    const sumPtr = lib.pardox_column_sum(salesRef[PTR_SYMBOL], 'price');
    if (sumPtr && lib.pardox_free_string) {
        const sumStr = lib.pardox_free_string(sumPtr, false);
        refSum = parseFloat(sumStr || '0');
    }
}

// Load customers.csv
const custRef = DataFrame.read_csv(CSV_CUST);
const custRows = custRef ? Number(custRef.shape[0]) : 0;
check('customers.csv loaded (50K rows)', custRows === 50000, `${custRows} rows`);

// =============================================================================
// Test: Export to PRDX (file:// equivalent)
// =============================================================================
console.log('\n[TEST] Export to PRDX...');

if (salesRef && salesRef[PTR_SYMBOL]) {
    const prdxPath = 'test_output.prdx';

    const writeResult = lib.pardox_to_prdx(salesRef[PTR_SYMBOL], prdxPath);
    check('pardox_to_prdx creates file', writeResult === 1 || writeResult > 0);

    if (fs.existsSync(prdxPath)) {
        const size = fs.statSync(prdxPath).size;
        check('PRDX file size > 0', size > 0, `${size} bytes`);

        // Read PRDX back using pardox_load_manager_prdx
        const mgr2 = lib.pardox_load_manager_prdx(prdxPath, 0);
        if (mgr2) {
            const rows2 = Number(lib.pardox_get_row_count(mgr2));
            check('Read PRDX returns 100K rows', rows2 === 100000, `${rows2} rows`);

            // Check sum
            const sumPtr2 = lib.pardox_column_sum(mgr2, 'price');
            if (sumPtr2 && lib.pardox_free_string) {
                const sumStr2 = lib.pardox_free_string(sumPtr2, false);
                const sum2 = parseFloat(sumStr2 || '0');
                const diff = Math.abs(sum2 - refSum);
                check('PRDX roundtrip sum matches', diff < 0.01, `diff=${diff.toFixed(2)}`);
            }

            lib.pardox_free_manager(mgr2);
        }

        // Cleanup
        try {
            fs.unlinkSync(prdxPath);
        } catch (e) { /* ignore */ }
    }
}

// =============================================================================
// Test: Cloud functions (if available)
// =============================================================================
if (hasCloudReadCsv) {
    console.log('\n[TEST] Cloud read CSV...');

    // Test with invalid credentials (max_retries=0)
    const invalidCreds = JSON.stringify({ max_retries: 0 });

    // Invalid path should return null
    const invalidUri = 'file://nonexistent.csv';
    const invalidMgr = lib.pardox_cloud_read_csv(invalidUri, '', '{}', invalidCreds);
    check('cloud_read_csv invalid path -> null', invalidMgr === null || invalidMgr === undefined);

    // Valid file path
    const validUri = `file://${CSV_SALES}`;
    const validMgr = lib.pardox_cloud_read_csv(validUri, '', '{}', '{}');
    if (validMgr) {
        const rows = Number(lib.pardox_get_row_count(validMgr));
        check('cloud_read_csv file:// returns 100K rows', rows === 100000, `${rows} rows`);
        lib.pardox_free_manager(validMgr);
    }

    // S3/GCS/Azure with invalid credentials should return null
    const s3Mgr = lib.pardox_cloud_read_csv('s3://bucket/key.csv', '', '{}', invalidCreds);
    check('s3:// with invalid creds -> null', s3Mgr === null || s3Mgr === undefined);

    const gsMgr = lib.pardox_cloud_read_csv('gs://bucket/key.csv', '', '{}', invalidCreds);
    check('gs:// with invalid creds -> null', gsMgr === null || gsMgr === undefined);

    const azMgr = lib.pardox_cloud_read_csv('az://container/key.csv', '', '{}', invalidCreds);
    check('az:// with invalid creds -> null', azMgr === null || azMgr === undefined);

    const ftpMgr = lib.pardox_cloud_read_csv('ftp://server/file.csv', '', '{}', invalidCreds);
    check('Unsupported scheme (ftp://) -> null', ftpMgr === null || ftpMgr === undefined);
}

// =============================================================================
// Test: Cloud PRDX functions (if available)
// =============================================================================
if (hasCloudWritePrdx && hasCloudReadPrdx && salesRef && salesRef[PTR_SYMBOL]) {
    console.log('\n[TEST] Cloud PRDX operations...');

    const prdxPath = 'cloud_test.prdx';
    const writeUri = `file://${prdxPath}`;

    const writeResult = lib.pardox_cloud_write_prdx(salesRef[PTR_SYMBOL], writeUri, '{}');
    check('cloud_write_prdx file:// succeeds', writeResult === 1);

    if (fs.existsSync(prdxPath)) {
        const readUri = `file://${prdxPath}`;
        const mgr = lib.pardox_cloud_read_prdx(readUri, 0, '{}');
        if (mgr) {
            const rows = Number(lib.pardox_get_row_count(mgr));
            check('cloud_read_prdx returns rows', rows > 0, `${rows} rows`);
            lib.pardox_free_manager(mgr);
        }
    }

    // Test with invalid credentials
    const invalidCreds = JSON.stringify({ max_retries: 0 });
    const s3WriteResult = lib.pardox_cloud_write_prdx(salesRef[PTR_SYMBOL], 's3://bucket/test.prdx', invalidCreds);
    check('cloud_write_prdx to s3:// with invalid creds -> negative', s3WriteResult < 0);

    try {
        fs.unlinkSync(prdxPath);
    } catch (e) { /* ignore */ }
}

// =============================================================================
// Summary
// =============================================================================
console.log('\n' + '='.repeat(60));
console.log('Gap 15 - Cloud Storage Results');
console.log('='.repeat(60));

const passed = results.filter(r => r.status === 'PASS').length;
const failed = results.filter(r => r.status === 'FAIL').length;
console.log(`  Results: ${passed}/${passed + failed} passed`);
console.log('='.repeat(60));

if (failed > 0) {
    console.log('\n  FAILED TESTS:');
    results.filter(r => r.status === 'FAIL').forEach(r => {
        console.log(`    - ${r.name}: ${r.detail}`);
    });
    process.exit(1);
} else {
    console.log('\n  ALL TESTS PASSED ✓');
    process.exit(0);
}