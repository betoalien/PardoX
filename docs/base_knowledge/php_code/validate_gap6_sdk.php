#!/usr/bin/env php
<?php
/**
 * validate_gap6_sdk.php - Gap 6: PostgreSQL Server (PG Wire Protocol)
 */

require_once __DIR__ . '/pardox/vendor/autoload.php';

$results = [];

function check(string $name, bool $cond, string $detail = ''): void {
    global $results;
    $status = $cond ? 'PASS' : 'FAIL';
    $results[] = [$name, $status, $detail];
    $mark = $cond ? '✓' : '✗';
    echo "  [$mark] $name" . ($detail ? "  - $detail" : '') . "\n";
}

echo "\n[TEST] PostgreSQL Server Functions...\n";

$ffi = \PardoX\Core\FFI::getInstance();

// Check if pardox PostgreSQL server is configured (Gap 6 is Python-based pg_server.py)
// This validates the PostgreSQL wire protocol support functions

// Check SQL functions that pg_server uses
try {
    $hasSqlQuery = $ffi->pardox_sql_query !== null;
} catch (\Throwable $e) { $hasSqlQuery = false; }
try {
    $hasExecuteSql = $ffi->pardox_execute_sql !== null;
} catch (\Throwable $e) { $hasExecuteSql = false; }
try {
    $hasHashJoin = $ffi->pardox_hash_join !== null;
} catch (\Throwable $e) { $hasHashJoin = false; }

check('pardox_sql_query exists (for pg_server)', $hasSqlQuery);
check('pardox_execute_sql exists (for pg_server)', $hasExecuteSql);
check('pardox_hash_join exists (for pg_server)', $hasHashJoin);

// Note: Gap 6 is implemented as a Python pg_server.py that speaks PostgreSQL wire protocol
// The actual server runs separately and exposes PostgreSQL compatibility
check('pg_server.py exists (Python implementation)', file_exists('pg_server.py'));

echo "\n" . str_repeat('=', 60) . "\n";
echo "Gap 6 - PostgreSQL Server Results\n";
echo str_repeat('=', 60) . "\n";

$passed = count(array_filter($results, fn($r) => $r[1] === 'PASS'));
$failed = count(array_filter($results, fn($r) => $r[1] === 'FAIL'));
echo "  Results: $passed/$passed+$failed passed\n";
echo str_repeat('=', 60) . "\n";

exit($failed > 0 ? 1 : 0);