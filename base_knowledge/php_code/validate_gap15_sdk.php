#!/usr/bin/env php
<?php
/**
 * validate_gap15_sdk.php - Gap 15: Cloud Storage Native (S3 / GCS / Azure)
 *
 * Tests pardox_cloud_read_csv, pardox_cloud_read_prdx, pardox_cloud_write_prdx
 */

require_once __DIR__ . '/pardox/vendor/autoload.php';

use PardoX\IO;
use PardoX\DataFrame;

$CSV_SALES = 'sales.csv';
$CSV_CUST  = 'customers.csv';

foreach ([$CSV_SALES, $CSV_CUST] as $f) {
    if (!file_exists($f)) {
        fwrite(STDERR, "Missing: $f\n");
        exit(1);
    }
}

$results = [];

function check(string $name, bool $cond, string $detail = ''): void {
    global $results;
    $status = $cond ? 'PASS' : 'FAIL';
    $results[] = [$name, $status, $detail];
    $mark = $cond ? '✓' : '✗';
    echo "  [$mark] $name" . ($detail ? "  - $detail" : '') . "\n";
}

echo "\n[TEST] Cloud Storage Functions...\n";

// Get FFI instance
$ffi = \PardoX\Core\FFI::getInstance();

// Test 1: Check if cloud functions exist by trying to call them
$hasCloudReadCsv = false;
$hasCloudReadPrdx = false;
$hasCloudWritePrdx = false;

// In PHP FFI, we check if the functions are defined in the library
// Using method_exists or try-catch since isset doesn't work with FFI objects
try {
    $testFunc = $ffi->pardox_cloud_read_csv;
    $hasCloudReadCsv = $testFunc !== null;
} catch (\Throwable $e) {
    $hasCloudReadCsv = false;
}

try {
    $testFunc = $ffi->pardox_cloud_read_prdx;
    $hasCloudReadPrdx = $testFunc !== null;
} catch (\Throwable $e) {
    $hasCloudReadPrdx = false;
}

try {
    $testFunc = $ffi->pardox_cloud_write_prdx;
    $hasCloudWritePrdx = $testFunc !== null;
} catch (\Throwable $e) {
    $hasCloudWritePrdx = false;
}

check('pardox_cloud_read_csv exists', $hasCloudReadCsv);
check('pardox_cloud_read_prdx exists', $hasCloudReadPrdx);
check('pardox_cloud_write_prdx exists', $hasCloudWritePrdx);

if ($hasCloudReadCsv) {
    // Test 4: file:// CSV read
    $uri = 'file://sales.csv';
    $mgr = $ffi->pardox_cloud_read_csv($uri, '', '{"delimiter":44,"has_header":true}', '{}');
    check('cloud_read_csv file:// returns manager', $mgr !== null);

    if ($mgr) {
        $rows = $ffi->pardox_get_row_count($mgr);
        check('cloud_read_csv returns 100K rows', $rows == 100000, "$rows rows");
        $ffi->pardox_free_manager($mgr);
    }
}

echo "\n" . str_repeat('=', 60) . "\n";
echo "Gap 15 - Cloud Storage Results\n";
echo str_repeat('=', 60) . "\n";

$passed = count(array_filter($results, fn($r) => $r[1] === 'PASS'));
$failed = count(array_filter($results, fn($r) => $r[1] === 'FAIL'));
echo "  Results: $passed/$passed+$failed passed\n";
echo str_repeat('=', 60) . "\n";

if ($failed > 0) {
    echo "\n  FAILED TESTS:\n";
    foreach (array_filter($results, fn($r) => $r[1] === 'FAIL') as $r) {
        echo "    - {$r[0]}: {$r[2]}\n";
    }
    exit(1);
} else {
    echo "\n  ALL TESTS PASSED ✓\n";
    exit(0);
}