#!/usr/bin/env node
/**
 * validate_gap6_sdk.js - Gap 6: PostgreSQL Server (PG Wire Protocol)
 */

const fs = require('fs');
const { getLib } = require('@pardox/pardox');

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

console.log('\n[TEST] Gap 6: PostgreSQL Server...\n');

// Check SQL functions that pg_server uses
const hasSqlQuery = lib.pardox_sql_query !== undefined && lib.pardox_sql_query !== null;
const hasExecuteSql = lib.pardox_execute_sql !== undefined && lib.pardox_execute_sql !== null;
const hasHashJoin = lib.pardox_hash_join !== undefined && lib.pardox_hash_join !== null;

check('pardox_sql_query exists (for pg_server)', hasSqlQuery);
check('pardox_execute_sql exists (for pg_server)', hasExecuteSql);
check('pardox_hash_join exists (for pg_server)', hasHashJoin);

// Note: Gap 6 is implemented as a Python pg_server.py that speaks PostgreSQL wire protocol
const pgServerPath = 'pg_server.py';
check('pg_server.py exists (Python implementation)', fs.existsSync(pgServerPath));

console.log('\n' + '='.repeat(60));
console.log('Gap 6 - PostgreSQL Server Results');
console.log('='.repeat(60));

const passed = results.filter(r => r.status === 'PASS').length;
const failed = results.filter(r => r.status === 'FAIL').length;
console.log(`  Results: ${passed}/${passed + failed} passed`);
console.log('='.repeat(60));

process.exit(failed > 0 ? 1 : 0);