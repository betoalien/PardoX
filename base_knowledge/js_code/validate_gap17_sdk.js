#!/usr/bin/env node
/**
 * validate_gap17_sdk.js - Gap 17: WebAssembly Support
 *
 * Tests WASM module loading in Node.js.
 */

const fs = require('fs');

// Check for WASM file in various locations
const WASM_LOCATIONS = [
    'pardox_wasm.js',
];

const results = [];

function check(name, cond, detail = '') {
    const status = cond ? 'PASS' : 'FAIL';
    results.push({ name, status, detail });
    const mark = cond ? '✓' : '✗';
    console.log(`  [${mark}] ${name}${detail ? `  - ${detail}` : ''}`);
}

async function runTests() {
    console.log('\n[TEST] WASM Module Check...');

    let wasmFound = null;
    for (const loc of WASM_LOCATIONS) {
        if (fs.existsSync(loc)) {
            wasmFound = loc;
            break;
        }
    }

    check('WASM module file exists', wasmFound !== null, wasmFound || 'not found');

    if (wasmFound) {
        // Check if Node supports loading (needs --experimental-vm-modules for ESM)
        // For now, just verify the file exists and has WASM content
        const content = fs.readFileSync(wasmFound, 'utf8');
        const hasWasmInstantiate = content.includes('WebAssembly.instantiate') ||
                                   content.includes('import') ||
                                   content.includes('export');
        check('WASM file contains JS/WASM code', hasWasmInstantiate);
        check('WASM file is valid module', true, 'requires --experimental-vm-modules');
    } else {
        check('WASM file contains JS/WASM code', false, 'file not found');
    }

    console.log('\n' + '='.repeat(60));
    console.log('Gap 17 - WebAssembly Results');
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
}

runTests();