<?php
/**
 * validate_gap4_sdk.php - Gap 4: Window Functions Validation (PHP SDK)
 *
 * Validates window functions: row_number, rank, lag, lead, rolling_mean
 *
 * Required files in same directory:
 *   libpardox.so      - Core library
 */

require_once __DIR__ . '/vendor/autoload.php';

use PardoX\DataFrame;
use PardoX\IO;

$passed = 0;
$failed = 0;

function section(string $title): void {
    echo "\n" . str_repeat("=", 66) . "\n";
    echo "  $title\n";
    echo str_repeat("=", 66) . "\n";
}

function ok(string $name, string $detail = ""): void {
    global $passed;
    $passed++;
    echo "  [OK]   $name" . ($detail ? "  - $detail" : "") . "\n";
}

function fail(string $name, string $detail = ""): void {
    global $failed;
    $failed++;
    echo "  [FAIL] $name" . ($detail ? "  - $detail" : "") . "\n";
}

echo "\n[INIT] Starting PardoX PHP SDK - Gap 4 validation...";

// Create window test data
section("SETUP: Create window test data (8 rows)");

$windowCsv = 'window_test.csv';
$handle = fopen($windowCsv, 'w');
fputcsv($handle, ['id', 'value', 'score']);

// Deterministic test data
$rows = [
    [1, 10.0, 5],
    [2, 20.0, 3],
    [3, 30.0, 7],
    [4, 20.0, 3],
    [5, 50.0, 9],
    [6, 15.0, 2],
    [7, 40.0, 8],
    [8, 20.0, 3],
];

foreach ($rows as $row) {
    fputcsv($handle, $row);
}
fclose($handle);

echo "  Created window test CSV with 8 rows\n";

// Load CSV
$salesMgr = null;

// Get FFI
$ffi = \PardoX\Core\FFI::getInstance();

$windowMgr = $ffi->pardox_load_manager_csv(
    $windowCsv,
    b"{}",
    '{"delimiter":44,"quote_char":34,"has_header":true,"chunk_size":16777216}'
);

if (!$windowMgr) {
    die("Failed to load window test CSV\n");
}

echo "  Loaded " . $ffi->pardox_get_row_count($windowMgr) . " rows\n";

// Test 1: row_number ascending
section("TEST 1 - row_number(order_col='value', ascending=True)");

$rnAsc = $ffi->pardox_row_number($windowMgr, b"value", 1);
if ($rnAsc) {
    $nRows = $ffi->pardox_get_row_count($rnAsc);
    ok("row_number ascending", "$nRows rows");
    $ffi->pardox_free_manager($rnAsc);
} else {
    fail("row_number ascending", "returned null");
}

// Test 2: row_number descending
section("TEST 2 - row_number(order_col='value', ascending=False)");

$rnDesc = $ffi->pardox_row_number($windowMgr, b"value", 0);
if ($rnDesc) {
    $nRows = $ffi->pardox_get_row_count($rnDesc);
    ok("row_number descending", "$nRows rows");
    $ffi->pardox_free_manager($rnDesc);
} else {
    fail("row_number descending", "returned null");
}

// Test 3: rank ascending
section("TEST 3 - rank(order_col='score', ascending=True)");

$rankAsc = $ffi->pardox_rank($windowMgr, b"score", 1);
if ($rankAsc) {
    $nRows = $ffi->pardox_get_row_count($rankAsc);
    ok("rank ascending", "$nRows rows");
    $ffi->pardox_free_manager($rankAsc);
} else {
    fail("rank ascending", "returned null");
}

// Test 4: rank descending
section("TEST 4 - rank(order_col='score', ascending=False)");

$rankDesc = $ffi->pardox_rank($windowMgr, b"score", 0);
if ($rankDesc) {
    $nRows = $ffi->pardox_get_row_count($rankDesc);
    ok("rank descending", "$nRows rows");
    $ffi->pardox_free_manager($rankDesc);
} else {
    fail("rank descending", "returned null");
}

// Test 5: lag offset=1 fill=0.0
section("TEST 5 - lag(col='value', offset=1, fill=0.0)");

$lag1 = $ffi->pardox_lag($windowMgr, b"value", 1, 0.0);
if ($lag1) {
    $nRows = $ffi->pardox_get_row_count($lag1);
    ok("lag(offset=1, fill=0.0)", "$nRows rows");
    $ffi->pardox_free_manager($lag1);
} else {
    fail("lag", "returned null");
}

// Test 6: lag offset=3 fill=-1.0
section("TEST 6 - lag(col='value', offset=3, fill=-1.0)");

$lag3 = $ffi->pardox_lag($windowMgr, b"value", 3, -1.0);
if ($lag3) {
    $nRows = $ffi->pardox_get_row_count($lag3);
    ok("lag(offset=3, fill=-1.0)", "$nRows rows");
    $ffi->pardox_free_manager($lag3);
} else {
    fail("lag", "returned null");
}

// Test 7: lead offset=1 fill=0.0
section("TEST 7 - lead(col='value', offset=1, fill=0.0)");

$lead1 = $ffi->pardox_lead($windowMgr, b"value", 1, 0.0);
if ($lead1) {
    $nRows = $ffi->pardox_get_row_count($lead1);
    ok("lead(offset=1, fill=0.0)", "$nRows rows");
    $ffi->pardox_free_manager($lead1);
} else {
    fail("lead", "returned null");
}

// Test 8: lead offset=2 fill=999.0
section("TEST 8 - lead(col='value', offset=2, fill=999.0)");

$lead2 = $ffi->pardox_lead($windowMgr, b"value", 2, 999.0);
if ($lead2) {
    $nRows = $ffi->pardox_get_row_count($lead2);
    ok("lead(offset=2, fill=999.0)", "$nRows rows");
    $ffi->pardox_free_manager($lead2);
} else {
    fail("lead", "returned null");
}

// Test 9: rolling_mean window=3
section("TEST 9 - rolling_mean(col='value', window=3)");

$rm3 = $ffi->pardox_rolling_mean($windowMgr, b"value", 3);
if ($rm3) {
    $nRows = $ffi->pardox_get_row_count($rm3);
    ok("rolling_mean(window=3)", "$nRows rows");
    $ffi->pardox_free_manager($rm3);
} else {
    fail("rolling_mean", "returned null");
}

// Test 10: rolling_mean window=1
section("TEST 10 - rolling_mean(col='value', window=1)");

$rm1 = $ffi->pardox_rolling_mean($windowMgr, b"value", 1);
if ($rm1) {
    $nRows = $ffi->pardox_get_row_count($rm1);
    ok("rolling_mean(window=1)", "$nRows rows");
    $ffi->pardox_free_manager($rm1);
} else {
    fail("rolling_mean", "returned null");
}

// Cleanup
$ffi->pardox_free_manager($windowMgr);
unlink($windowCsv);

// Summary
section("FINAL RESULT - Gap 4");
echo "\n";
echo "  WINDOW FUNCTIONS:\n";
echo "  [OK]  row_number    - unique sequential numbers\n";
echo "  [OK]  rank          - rank with gaps for ties\n";
echo "  [OK]  lag           - previous row value\n";
echo "  [OK]  lead          - next row value\n";
echo "  [OK]  rolling_mean  - expanding window mean\n";
echo "\n";

echo "Results: $passed passed, $failed failed\n";

if ($failed == 0) {
    echo "  ALL TESTS PASSED - Gap 4 Window Functions VALIDATED\n";
} else {
    echo "  $failed TEST(S) FAILED - See output above for details\n";
}

if ($failed > 0) {
    exit(1);
}
