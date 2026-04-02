#!/usr/bin/env php
<?php
/**
 * validate_gap28_sdk.php - Gap 28: Linear Algebra
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

echo "\n[TEST] Linear Algebra Functions...\n";

$ffi = \PardoX\Core\FFI::getInstance();

// Check functions
try {
    $hasCosineSim = $ffi->pardox_cosine_sim !== null;
} catch (\Throwable $e) { $hasCosineSim = false; }
try {
    $hasMatmul = $ffi->pardox_matmul !== null;
} catch (\Throwable $e) { $hasMatmul = false; }
try {
    $hasL2Normalize = $ffi->pardox_l2_normalize !== null;
} catch (\Throwable $e) { $hasL2Normalize = false; }
try {
    $hasL1Normalize = $ffi->pardox_l1_normalize !== null;
} catch (\Throwable $e) { $hasL1Normalize = false; }
try {
    $hasPca = $ffi->pardox_pca !== null;
} catch (\Throwable $e) { $hasPca = false; }

check('pardox_cosine_sim exists', $hasCosineSim);
check('pardox_matmul exists', $hasMatmul);
check('pardox_l2_normalize exists', $hasL2Normalize);
check('pardox_l1_normalize exists', $hasL1Normalize);
check('pardox_pca exists', $hasPca);

echo "\n" . str_repeat('=', 60) . "\n";
echo "Gap 28 - Linear Algebra Results\n";
echo str_repeat('=', 60) . "\n";

$passed = count(array_filter($results, fn($r) => $r[1] === 'PASS'));
$failed = count(array_filter($results, fn($r) => $r[1] === 'FAIL'));
echo "  Results: $passed/$passed+$failed passed\n";
echo str_repeat('=', 60) . "\n";

exit($failed > 0 ? 1 : 0);