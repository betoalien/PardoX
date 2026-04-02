#!/usr/bin/env php
<?php
/**
 * validate_gap22_sdk.php - Gap 22: Distributed Computing
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

echo "\n[TEST] Distributed Computing Functions...\n";

$ffi = \PardoX\Core\FFI::getInstance();

// Check functions
try {
    $hasClusterConnect = $ffi->pardox_cluster_connect !== null;
} catch (\Throwable $e) { $hasClusterConnect = false; }
try {
    $hasClusterScatter = $ffi->pardox_cluster_scatter !== null;
} catch (\Throwable $e) { $hasClusterScatter = false; }
try {
    $hasClusterSql = $ffi->pardox_cluster_sql !== null;
} catch (\Throwable $e) { $hasClusterSql = false; }
try {
    $hasClusterPing = $ffi->pardox_cluster_ping !== null;
} catch (\Throwable $e) { $hasClusterPing = false; }

check('pardox_cluster_connect exists', $hasClusterConnect);
check('pardox_cluster_scatter exists', $hasClusterScatter);
check('pardox_cluster_sql exists', $hasClusterSql);
check('pardox_cluster_ping exists', $hasClusterPing);

echo "\n" . str_repeat('=', 60) . "\n";
echo "Gap 22 - Distributed Computing Results\n";
echo str_repeat('=', 60) . "\n";

$passed = count(array_filter($results, fn($r) => $r[1] === 'PASS'));
$failed = count(array_filter($results, fn($r) => $r[1] === 'FAIL'));
echo "  Results: $passed/$passed+$failed passed\n";
echo str_repeat('=', 60) . "\n";

exit($failed > 0 ? 1 : 0);