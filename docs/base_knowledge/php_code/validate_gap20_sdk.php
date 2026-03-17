#!/usr/bin/env php
<?php
/**
 * validate_gap20_sdk.php - Gap 20: Time Travel
 */

require_once __DIR__ . '/pardox/vendor/autoload.php';

$CSV_SALES = 'sales.csv';

if (!file_exists($CSV_SALES)) {
    fwrite(STDERR, "Missing: $CSV_SALES\n");
    exit(1);
}

$results = [];

function check(string $name, bool $cond, string $detail = ''): void {
    global $results;
    $status = $cond ? 'PASS' : 'FAIL';
    $results[] = [$name, $status, $detail];
    $mark = $cond ? '✓' : '✗';
    echo "  [$mark] $name" . ($detail ? "  - $detail" : '') . "\n";
}

echo "\n[TEST] Time Travel Functions...\n";

$ffi = \PardoX\Core\FFI::getInstance();

// Check functions
try {
    $hasVersionWrite = $ffi->pardox_version_write !== null;
} catch (\Throwable $e) { $hasVersionWrite = false; }
try {
    $hasVersionRead = $ffi->pardox_version_read !== null;
} catch (\Throwable $e) { $hasVersionRead = false; }
try {
    $hasVersionDelete = $ffi->pardox_version_delete !== null;
} catch (\Throwable $e) { $hasVersionDelete = false; }
try {
    $hasVersionList = $ffi->pardox_version_list !== null;
} catch (\Throwable $e) { $hasVersionList = false; }

check('pardox_version_write exists', $hasVersionWrite);
check('pardox_version_read exists', $hasVersionRead);
check('pardox_version_delete exists', $hasVersionDelete);
check('pardox_version_list exists', $hasVersionList);

echo "\n" . str_repeat('=', 60) . "\n";
echo "Gap 20 - Time Travel Results\n";
echo str_repeat('=', 60) . "\n";

$passed = count(array_filter($results, fn($r) => $r[1] === 'PASS'));
$failed = count(array_filter($results, fn($r) => $r[1] === 'FAIL'));
echo "  Results: $passed/$passed+$failed passed\n";
echo str_repeat('=', 60) . "\n";

exit($failed > 0 ? 1 : 0);