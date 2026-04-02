#!/usr/bin/env node
/**
 * validate_gap12_sdk.js - Gap 12: PRDX Loader
 */

const fs = require('fs');
const { getLib } = require('@pardox/pardox');
const PRDX_PATH = 'ventas_consolidado.prdx';

if (!fs.existsSync(PRDX_PATH)) {
    console.error(`Missing: ${PRDX_PATH}`);
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

console.log('\n[TEST] Gap 12: PRDX Loader...\n');

// Check PRDX loader function
const hasLoadPrdx = lib.pardox_load_manager_prdx !== undefined && lib.pardox_load_manager_prdx !== null;
check('pardox_load_manager_prdx exists', hasLoadPrdx);

if (hasLoadPrdx) {
    const mgr = lib.pardox_load_manager_prdx(PRDX_PATH, 10000);
    check('Load PRDX file', mgr !== undefined && mgr !== null);

    if (mgr) {
        const rows = lib.pardox_get_row_count(mgr);
        check('PRDX has rows', rows > 0, `${rows} rows`);
        lib.pardox_free_manager(mgr);
    }
}

console.log('\n' + '='.repeat(60));
console.log('Gap 12 - PRDX Loader Results');
console.log('='.repeat(60));

const passed = results.filter(r => r.status === 'PASS').length;
const failed = results.filter(r => r.status === 'FAIL').length;
console.log(`  Results: ${passed}/${passed + failed} passed`);
console.log('='.repeat(60));

process.exit(failed > 0 ? 1 : 0);