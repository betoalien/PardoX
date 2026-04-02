#!/usr/bin/env php
<?php
/**
 * validate_gap26_sdk.php - Gap 26: Fault Tolerance
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

echo "\n[TEST] Fault Tolerance Functions...\n";

$ffi = \PardoX\Core\FFI::getInstance();

// Check functions
try {
    $hasScatterResilient = $ffi->pardox_cluster_scatter_resilient !== null;
} catch (\Throwable $e) { $hasScatterResilient = false; }
try {
    $hasSqlResilient = $ffi->pardox_cluster_sql_resilient !== null;
} catch (\Throwable $e) { $hasSqlResilient = false; }
try {
    $hasCheckpoint = $ffi->pardox_cluster_checkpoint !== null;
} catch (\Throwable $e) { $hasCheckpoint = false; }

check('pardox_cluster_scatter_resilient exists', $hasScatterResilient);
check('pardox_cluster_sql_resilient exists', $hasSqlResilient);
check('pardox_cluster_checkpoint exists', $hasCheckpoint);

echo "\n" . str_repeat('=', 60) . "\n";
echo "Gap 26 - Fault Tolerance Results\n";
echo str_repeat('=', 60) . "\n";

$passed = count(array_filter($results, fn($r) => $r[1] === 'PASS'));
$failed = count(array_filter($results, fn($r) => $r[1] === 'FAIL'));
echo "  Results: $passed/$passed+$failed passed\n";
echo str_repeat('=', 60) . "\n";

exit($failed > 0 ? 1 : 0);