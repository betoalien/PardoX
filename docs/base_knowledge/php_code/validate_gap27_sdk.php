#!/usr/bin/env php
<?php
/**
 * validate_gap27_sdk.php - Gap 27: Query Planner
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

echo "\n[TEST] Query Planner Functions...\n";

$ffi = \PardoX\Core\FFI::getInstance();

// Check functions
try {
    $hasLazyOptimize = $ffi->pardox_lazy_optimize !== null;
} catch (\Throwable $e) { $hasLazyOptimize = false; }
try {
    $hasLazyStats = $ffi->pardox_lazy_stats !== null;
} catch (\Throwable $e) { $hasLazyStats = false; }
try {
    $hasLazyScanPrdx = $ffi->pardox_lazy_scan_prdx !== null;
} catch (\Throwable $e) { $hasLazyScanPrdx = false; }

check('pardox_lazy_optimize exists', $hasLazyOptimize);
check('pardox_lazy_stats exists', $hasLazyStats);
check('pardox_lazy_scan_prdx exists', $hasLazyScanPrdx);

echo "\n" . str_repeat('=', 60) . "\n";
echo "Gap 27 - Query Planner Results\n";
echo str_repeat('=', 60) . "\n";

$passed = count(array_filter($results, fn($r) => $r[1] === 'PASS'));
$failed = count(array_filter($results, fn($r) => $r[1] === 'FAIL'));
echo "  Results: $passed/$passed+$failed passed\n";
echo str_repeat('=', 60) . "\n";

exit($failed > 0 ? 1 : 0);