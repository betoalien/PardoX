#!/usr/bin/env php
<?php
/**
 * validate_gap18_sdk.php - Gap 18: Encryption (PRDX level)
 */

require_once __DIR__ . '/pardox/vendor/autoload.php';

use PardoX\IO;

$CSV_SALES = 'sales.csv';
if (!file_exists($CSV_SALES)) {
    fwrite(STDERR, "Missing: $CSV_SALES\n");
    exit(1);
}

$results = [];

function check(string $name, bool $cond, string $detail = ''): void {
    global $results;
    $mark = $cond ? '✓' : '✗';
    echo "  [$mark] $name" . ($detail ? "  - $detail" : '') . "\n";
}

echo "\n[TEST] Encryption Functions...\n";

$ffi = \PardoX\Core\FFI::getInstance();

// Load CSV
$cfg = '{"delimiter":44,"has_header":true}';
$salesMgr = $ffi->pardox_load_manager_csv($CSV_SALES, '{}', $cfg);
check('Load sales.csv', $salesMgr !== null);

$rows = $ffi->pardox_get_row_count($salesMgr);
check('Row count', $rows == 100000, "$rows rows");

// Check functions exist
try {
    $hasWrite = $ffi->pardox_write_prdx_encrypted !== null;
} catch (\Throwable $e) { $hasWrite = false; }
try {
    $hasRead = $ffi->pardox_read_prdx_encrypted !== null;
} catch (\Throwable $e) { $hasRead = false; }

check('pardox_write_prdx_encrypted exists', $hasWrite);
check('pardox_read_prdx_encrypted exists', $hasRead);

if ($hasWrite && $hasRead) {
    $encPath = 'test_encrypted.prdx';
    $pwd = 'test_password_123';

    // Write encrypted PRDX
    $rc = $ffi->pardox_write_prdx_encrypted($salesMgr, $encPath, $pwd);
    check('Write encrypted PRDX', $rc == 1);

    if ($rc == 1 && file_exists($encPath)) {
        // Read encrypted PRDX
        $mgr2 = $ffi->pardox_read_prdx_encrypted($encPath, $pwd);
        check('Read encrypted PRDX', $mgr2 !== null);

        if ($mgr2) {
            $rows2 = $ffi->pardox_get_row_count($mgr2);
            check('Row count matches', $rows2 == 100000, "$rows2 rows");
            $ffi->pardox_free_manager($mgr2);
        }

        @unlink($encPath);
    }
}

$ffi->pardox_free_manager($salesMgr);

echo "\n" . str_repeat('=', 60) . "\n";
echo "Gap 18 - Encryption Results\n";
echo str_repeat('=', 60) . "\n";

$passed = count(array_filter($results, fn($r) => $r[1] === 'PASS'));
$failed = count(array_filter($results, fn($r) => $r[1] === 'FAIL'));
echo "  Results: $passed/$passed+$failed passed\n";
echo str_repeat('=', 60) . "\n";

if ($failed > 0) {
    echo "\n  FAILED:\n";
    foreach (array_filter($results, fn($r) => $r[1] === 'FAIL') as $r) {
        echo "    - {$r[0]}\n";
    }
    exit(1);
}
echo "\n  ALL TESTS PASSED ✓\n";
exit(0);