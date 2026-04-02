#!/usr/bin/env php
<?php
/**
 * validate_gap24_sdk.php - Gap 24: JOINs in pg_server
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

echo "\n[TEST] PostgreSQL JOIN Functions...\n";

$ffi = \PardoX\Core\FFI::getInstance();

// Check functions
try {
    $hasHashJoin = $ffi->pardox_hash_join !== null;
} catch (\Throwable $e) { $hasHashJoin = false; }

check('pardox_hash_join exists', $hasHashJoin);

echo "\n" . str_repeat('=', 60) . "\n";
echo "Gap 24 - JOINs in pg_server Results\n";
echo str_repeat('=', 60) . "\n";

$passed = count(array_filter($results, fn($r) => $r[1] === 'PASS'));
$failed = count(array_filter($results, fn($r) => $r[1] === 'FAIL'));
echo "  Results: $passed/$passed+$failed passed\n";
echo str_repeat('=', 60) . "\n";

exit($failed > 0 ? 1 : 0);