#!/usr/bin/env node
/**
 * validate_gap18_sdk.js - Gap 18: Encryption
 *
 * Tests encryption functions: pardox_write_prdx_encrypted, pardox_read_prdx_encrypted
 */

const fs = require('fs');
const { getLib } = require('@pardox/pardox');
const { DataFrame } = require('@pardox/pardox');

const CSV_SALES = 'sales.csv';

if (!fs.existsSync(CSV_SALES)) {
    console.error(`Missing: ${CSV_SALES}`);
    process.exit(1);
}

let lib;
try {
    lib = getLib();
} catch (err) {
    console.error(`Fatal: ${err.message}`);
    process.exit(1);
}

const results = [];

function check(name, cond, detail = '') {
    const status = cond ? 'PASS' : 'FAIL';
    results.push({ name, status, detail });
    const mark = cond ? '✓' : '✗';
    console.log(`  [${mark}] ${name}${detail ? `  - ${detail}` : ''}`);
}

console.log('\n[TEST] Loading data...');
const df = DataFrame.read_csv(CSV_SALES);
check('CSV loaded', df && Number(df.shape[0]) === 100000, `${df ? Number(df.shape[0]) : 0} rows`);

console.log('\n[TEST] Encryption API...');
const hasWriteEncrypted = lib.pardox_write_prdx_encrypted !== undefined && lib.pardox_write_prdx_encrypted !== null;
const hasReadEncrypted = lib.pardox_read_prdx_encrypted !== undefined && lib.pardox_read_prdx_encrypted !== null;

check('pardox_write_prdx_encrypted exists', hasWriteEncrypted);
check('pardox_read_prdx_encrypted exists', hasReadEncrypted);

if (hasWriteEncrypted && hasReadEncrypted && df) {
    const PTR_SYMBOL = Symbol.for('pardox.ptr');
    const mgr = df[PTR_SYMBOL];
    const encPath = 'test_encrypted.prdx';
    const pwd = 'test_password_123';

    // Test encrypted PRDX write
    const encResult = lib.pardox_write_prdx_encrypted(mgr, encPath, pwd);
    check('Write encrypted PRDX succeeds', encResult === 1);

    // Test encrypted PRDX read
    if (encResult === 1 && fs.existsSync(encPath)) {
        const mgr2 = lib.pardox_read_prdx_encrypted(encPath, pwd);
        check('Read encrypted PRDX succeeds', mgr2 !== undefined && mgr2 !== null);
        if (mgr2) lib.pardox_free_manager(mgr2);
        fs.unlinkSync(encPath);
    }
} else {
    check('Write encrypted PRDX succeeds', false);
    check('Read encrypted PRDX succeeds', false);
}

console.log('\n' + '='.repeat(60));
console.log('Gap 18 - Encryption Results');
console.log('='.repeat(60));

const passed = results.filter(r => r.status === 'PASS').length;
const failed = results.filter(r => r.status === 'FAIL').length;
console.log(`  Results: ${passed}/${passed + failed} passed`);
console.log('='.repeat(60));

if (failed > 0) {
    results.filter(r => r.status === 'FAIL').forEach(r => {
        console.log(`    - ${r.name}: ${r.detail}`);
    });
    process.exit(1);
} else {
    console.log('\n  ALL TESTS PASSED ✓');
    process.exit(0);
}