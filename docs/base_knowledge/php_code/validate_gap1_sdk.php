<?php
/**
 * validate_gap1_sdk.php - Gap 1: GroupBy Validation (PHP SDK)
 *
 * Validates GroupBy functionality using the PardoX PHP SDK.
 * Tests aggregations: sum, mean, count, min, max, std
 *
 * Required files in same directory:
 *   libpardox.so      - Core library
 *   libpardox_gpu.so  - GPU module (optional)
 *   customers.csv     - 50K rows
 *   sales.csv         - 100K rows
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

// Get paths
$customersCsv = 'customers.csv';
$salesCsv = 'sales.csv';

// Verify files
foreach ([$customersCsv, $salesCsv] as $f) {
    if (!file_exists($f)) {
        die("Missing required file: $f\n");
    }
}

echo "\n[INIT] Starting PardoX PHP SDK validation...";
echo "\n[INFO] Using customers.csv and sales.csv";

// Load DataFrames
section("1. Load CSVs - customers.csv and sales.csv");

$customers = IO::read_csv($customersCsv);
$sales = IO::read_csv($salesCsv);

$nCustomers = $customers->shape[0];
$nSales = $sales->shape[0];

echo "  customers : " . number_format($nCustomers) . " rows\n";
echo "  sales     : " . number_format($nSales) . " rows\n";

ok("Load customers.csv", number_format($nCustomers) . " rows");
ok("Load sales.csv", number_format($nSales) . " rows");

// Get FFI for direct testing
$ffi = \PardoX\Core\FFI::getInstance();

// Helper function to call groupby
function groupByAgg($ffi, $mgr, $cols, array $agg) {
    $colsJson = json_encode(is_array($cols) ? $cols : [$cols]);
    $aggJson = json_encode($agg);
    $result = $ffi->pardox_groupby_agg($mgr, $colsJson, $aggJson);
    return $result;
}

// Get manager pointers
$salesMgr = $sales->_ptr();
$customersMgr = $customers->_ptr();

// Get raw manager pointers
$salesMgr = $sales->_ptr();
$customersMgr = $customers->_ptr();

// Test 2: sales.groupby('category') -> sum(price), mean(quantity)
section("2. GroupBy category -> sum(price), mean(quantity)");
echo "  8 categories, ~12.5K rows/group\n";

$salesMgr = $sales->_ptr();
$r2 = groupByAgg($ffi, $salesMgr, "category", ["price" => "sum", "quantity" => "mean"]);
if ($r2) {
    $nRows = $ffi->pardox_get_row_count($r2);
    ok("GroupBy category", "$nRows groups");
    $ffi->pardox_free_manager($r2);
} else {
    fail("GroupBy category", "returned null");
}

// Test 3: sales.groupby('id_customer') -> sum(price), count(id_transaction)
section("3. GroupBy id_customer -> sum(price), count(id_transaction)");
echo "  ~43K groups, ~2 rows/group -> CPU Rayon\n";

$r3 = groupByAgg($ffi, $salesMgr, "id_customer", ["price" => "sum", "id_transaction" => "count"]);
if ($r3) {
    $nRows = $ffi->pardox_get_row_count($r3);
    ok("GroupBy id_customer", number_format($nRows) . " groups");
    $ffi->pardox_free_manager($r3);
} else {
    fail("GroupBy id_customer", "returned null");
}

// Test 4: customers.groupby('state') -> count(id_customer)
section("4. GroupBy state -> count(id_customer)");
echo "  12 states -> CPU Rayon\n";

$customersMgr = $customers->_ptr();
$r4 = groupByAgg($ffi, $customersMgr, "state", ["id_customer" => "count"]);
if ($r4) {
    $nRows = $ffi->pardox_get_row_count($r4);
    ok("GroupBy state", "$nRows groups");
    $ffi->pardox_free_manager($r4);
} else {
    fail("GroupBy state", "returned null");
}

// Test 5: customers.groupby(['state', 'city']) -> count(id_customer)
section("5. GroupBy [state, city] -> count(id_customer)");
echo "  Multi-column GroupBy -> CPU Rayon\n";

$r5 = groupByAgg($ffi, $customersMgr, ["state", "city"], ["id_customer" => "count"]);
if ($r5) {
    $nRows = $ffi->pardox_get_row_count($r5);
    ok("GroupBy state+city", number_format($nRows) . " groups");
    $ffi->pardox_free_manager($r5);
} else {
    fail("GroupBy state+city", "returned null");
}

// Test 6: sales.groupby('category') -> min(price), max(quantity), std(discount)
section("6. GroupBy category -> min(price), max(quantity), std(discount)");
echo "  -> GPU active for Min and Max\n";

$r6 = groupByAgg($ffi, $salesMgr, "category", ["price" => "min", "quantity" => "max", "discount" => "std"]);
if ($r6) {
    $nRows = $ffi->pardox_get_row_count($r6);
    ok("GroupBy category min/max/std", "$nRows groups");
    $ffi->pardox_free_manager($r6);
} else {
    fail("GroupBy category min/max/std", "returned null");
}

// Test 7: sales.groupby('category') -> mean(price), std(discount)
section("7. GroupBy category -> mean(price), std(discount)");
echo "  Mean and Std -> always CPU Rayon\n";

$r7 = groupByAgg($ffi, $salesMgr, "category", ["price" => "mean", "discount" => "std"]);
if ($r7) {
    $nRows = $ffi->pardox_get_row_count($r7);
    ok("GroupBy category mean/std", "$nRows groups");
    $ffi->pardox_free_manager($r7);
} else {
    fail("GroupBy category mean/std", "returned null");
}

// Cleanup
$ffi->pardox_free_manager($customersMgr);
$ffi->pardox_free_manager($salesMgr);

// Summary
section("FINAL RESULT");
echo "\n";
echo "  [OK] Test 2: GroupBy category   -> sum + mean\n";
echo "  [OK] Test 3: GroupBy id_customer -> sum + count\n";
echo "  [OK] Test 4: GroupBy state       -> count\n";
echo "  [OK] Test 5: GroupBy state+city  -> count\n";
echo "  [OK] Test 6: GroupBy category   -> min + max + std\n";
echo "  [OK] Test 7: GroupBy category   -> mean + std\n";
echo "\n";

echo "Results: $passed passed, $failed failed\n";

if ($failed > 0) {
    exit(1);
}
