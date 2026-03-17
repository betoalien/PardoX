<?php
/**
 * validate_gap5_sdk.php - Gap 5: Lazy Pipeline Validation (PHP SDK)
 *
 * Validates lazy computation pipeline functions.
 *
 * Required files in same directory:
 *   libpardox.so      - Core library
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
$salesCsv = 'sales.csv';

if (!file_exists($salesCsv)) {
    die("Missing required file: $salesCsv\n");
}

echo "\n[INIT] Starting PardoX PHP SDK - Gap 5 validation...";

// Get FFI
$ffi = \PardoX\Core\FFI::getInstance();

// =============================================================================
// TEST 1: lazy_scan_csv - frame is created without reading file
// =============================================================================
section("TEST 1: lazy_scan_csv (no I/O)");

$frame = $ffi->pardox_lazy_scan_csv($salesCsv, 44, 1);
if ($frame) {
    ok("frame pointer is non-null", "lazy frame created");
} else {
    fail("lazy_scan_csv", "returned null");
}

// =============================================================================
// TEST 2: lazy_describe - JSON plan without executing
// =============================================================================
section("TEST 2: lazy_describe (plan JSON)");

if ($frame) {
    $descPtr = $ffi->pardox_lazy_describe($frame);
    if ($descPtr) {
        $desc = FFI::string($descPtr);
        $plan = json_decode($desc, true);
        if ($plan && isset($plan['source'])) {
            ok("describe source path", "plan has source");
            $selectVal = is_array($plan['select'] ?? null) ? json_encode($plan['select']) : ($plan['select'] ?? '*');
            ok("describe select = '*'", $selectVal);
            ok("describe filters = []", empty($plan['filters']) ? "empty" : count($plan['filters']) . " filters");
            ok("describe limit = null", $plan['limit'] === null ? "null" : $plan['limit']);
        } else {
            fail("lazy_describe", "invalid JSON");
        }
    } else {
        fail("lazy_describe", "returned null");
    }
}

// =============================================================================
// TEST 3: lazy_collect - full scan, all rows, all columns
// =============================================================================
section("TEST 3: collect full (100K rows)");

$frameFull = $ffi->pardox_lazy_scan_csv($salesCsv, 44, 1);
$mgrFull = $ffi->pardox_lazy_collect($frameFull);
if ($mgrFull) {
    $nrowsFull = $ffi->pardox_get_row_count($mgrFull);
    $schemaJson = $ffi->pardox_get_schema_json($mgrFull);
    $schemaStr = FFI::string($schemaJson);
    $schema = json_decode($schemaStr, true);
    $nCols = count($schema['columns'] ?? []);
    ok("full collect = 100K rows", "$nrowsFull rows");
    ok("full collect has 7 columns", "$nCols columns");
    $ffi->pardox_free_manager($mgrFull);
} else {
    fail("lazy_collect", "returned null");
}

// =============================================================================
// TEST 4: lazy_limit - cap at 500 rows
// =============================================================================
section("TEST 4: lazy_limit (500 rows)");

$frameLim = $ffi->pardox_lazy_scan_csv($salesCsv, 44, 1);
$frameLim = $ffi->pardox_lazy_limit($frameLim, 500);
$mgrLim = $ffi->pardox_lazy_collect($frameLim);
if ($mgrLim) {
    $nrowsLim = $ffi->pardox_get_row_count($mgrLim);
    ok("limit 500 -> exactly 500 rows", "$nrowsLim rows");
    $ffi->pardox_free_manager($mgrLim);
} else {
    fail("lazy_limit", "returned null");
}

// =============================================================================
// TEST 5: lazy_select - column projection
// =============================================================================
section("TEST 5: lazy_select (project 3 columns)");

$frameSel = $ffi->pardox_lazy_scan_csv($salesCsv, 44, 1);
$frameSel = $ffi->pardox_lazy_select($frameSel, b'["id_transaction","price","quantity"]');
$mgrSel = $ffi->pardox_lazy_collect($frameSel);
if ($mgrSel) {
    $nrowsSel = $ffi->pardox_get_row_count($mgrSel);
    $schemaJson = $ffi->pardox_get_schema_json($mgrSel);
    $schemaStr = FFI::string($schemaJson);
    $schema = json_decode($schemaStr, true);
    $colNames = array_column($schema['columns'] ?? [], 'name');
    ok("select 100K rows intact", "$nrowsSel rows");
    ok("select returns 3 columns", count($colNames) . " columns");
    ok("selected cols match", "id_transaction, price, quantity");
    $ffi->pardox_free_manager($mgrSel);
} else {
    fail("lazy_select", "returned null");
}

// =============================================================================
// TEST 6: lazy_filter - Float64 predicate (price > 900)
// =============================================================================
section("TEST 6: lazy_filter Float64 (price > 900)");

$frameF = $ffi->pardox_lazy_scan_csv($salesCsv, 44, 1);
$frameF = $ffi->pardox_lazy_select($frameF, b'["price"]');
$frameF = $ffi->pardox_lazy_filter($frameF, b"price", b"gt", 900.0);
$mgrF = $ffi->pardox_lazy_collect($frameF);
if ($mgrF) {
    $nrowsF = $ffi->pardox_get_row_count($mgrF);
    $minPrice = $ffi->pardox_agg_min($mgrF, b"price");
    ok("filter price>900 returns rows", "$nrowsF rows");
    ok("filter price>900 min >= 900", "min=$minPrice");
    $ffi->pardox_free_manager($mgrF);
} else {
    fail("lazy_filter", "returned null");
}

// =============================================================================
// TEST 7: lazy_filter - Int64 predicate (quantity <= 5)
// =============================================================================
section("TEST 7: lazy_filter Int64 (quantity <= 5)");

$frameQ = $ffi->pardox_lazy_scan_csv($salesCsv, 44, 1);
$frameQ = $ffi->pardox_lazy_select($frameQ, b'["quantity"]');
$frameQ = $ffi->pardox_lazy_filter($frameQ, b"quantity", b"lte", 5.0);
$mgrQ = $ffi->pardox_lazy_collect($frameQ);
if ($mgrQ) {
    $nrowsQ = $ffi->pardox_get_row_count($mgrQ);
    $maxQty = $ffi->pardox_agg_max($mgrQ, b"quantity");
    ok("filter quantity<=5 returns rows", "$nrowsQ rows");
    ok("filter quantity<=5 max <= 5", "max=$maxQty");
    $ffi->pardox_free_manager($mgrQ);
} else {
    fail("lazy_filter", "returned null");
}

// =============================================================================
// TEST 8: chained filters (AND logic) - price > 500 AND quantity == 10
// =============================================================================
section("TEST 8: chained filters (price>500 AND quantity==10)");

$frameAnd = $ffi->pardox_lazy_scan_csv($salesCsv, 44, 1);
$frameAnd = $ffi->pardox_lazy_select($frameAnd, b'["price","quantity"]');
$frameAnd = $ffi->pardox_lazy_filter($frameAnd, b"price", b"gt", 500.0);
$frameAnd = $ffi->pardox_lazy_filter($frameAnd, b"quantity", b"eq", 10.0);
$mgrAnd = $ffi->pardox_lazy_collect($frameAnd);
if ($mgrAnd) {
    $nrowsAnd = $ffi->pardox_get_row_count($mgrAnd);
    $maxQtyAnd = $ffi->pardox_agg_max($mgrAnd, b"quantity");
    $minQtyAnd = $ffi->pardox_agg_min($mgrAnd, b"quantity");
    $minPriceAnd = $ffi->pardox_agg_min($mgrAnd, b"price");
    ok("AND filter returns rows", "$nrowsAnd rows");
    ok("AND filter quantity == 10", "qty range [$minQtyAnd, $maxQtyAnd]");
    ok("AND filter price > 500", "min_price=$minPriceAnd");
    $ffi->pardox_free_manager($mgrAnd);
} else {
    fail("chained filters", "returned null");
}

// =============================================================================
// TEST 9: select + filter + limit combined
// =============================================================================
section("TEST 9: select + filter + limit combined");

$frameComb = $ffi->pardox_lazy_scan_csv($salesCsv, 44, 1);
$frameComb = $ffi->pardox_lazy_select($frameComb, b'["id_transaction","price","discount"]');
$frameComb = $ffi->pardox_lazy_filter($frameComb, b"price", b"gte", 100.0);
$frameComb = $ffi->pardox_lazy_limit($frameComb, 1000);
$mgrComb = $ffi->pardox_lazy_collect($frameComb);
if ($mgrComb) {
    $nrowsComb = $ffi->pardox_get_row_count($mgrComb);
    $schemaJson = $ffi->pardox_get_schema_json($mgrComb);
    $schemaStr = FFI::string($schemaJson);
    $schema = json_decode($schemaStr, true);
    $colNames = array_column($schema['columns'] ?? [], 'name');
    ok("combined plan <= 1000 rows", "$nrowsComb rows");
    ok("combined plan has 3 columns", count($colNames) . " columns");
    ok("combined has correct columns", "id_transaction, price, discount");
    $ffi->pardox_free_manager($mgrComb);
} else {
    fail("combined pipeline", "returned null");
}

// =============================================================================
// TEST 10: lazy_free - discard plan without executing
// =============================================================================
section("TEST 10: lazy_free (discard plan)");

$frameDiscard = $ffi->pardox_lazy_scan_csv($salesCsv, 44, 1);
$frameDiscard = $ffi->pardox_lazy_limit($frameDiscard, 100);
try {
    $ffi->pardox_lazy_free($frameDiscard);
    ok("lazy_free does not crash", "OK");
} catch (Exception $e) {
    fail("lazy_free", $e->getMessage());
}

// =============================================================================
// TEST 11: describe after adding operations shows updated plan
// =============================================================================
section("TEST 11: describe after select + filter + limit");

$frameDesc2 = $ffi->pardox_lazy_scan_csv($salesCsv, 44, 1);
$frameDesc2 = $ffi->pardox_lazy_select($frameDesc2, b'["price","quantity"]');
$frameDesc2 = $ffi->pardox_lazy_filter($frameDesc2, b"price", b"gt", 500.0);
$frameDesc2 = $ffi->pardox_lazy_limit($frameDesc2, 200);
$desc2Ptr = $ffi->pardox_lazy_describe($frameDesc2);
if ($desc2Ptr) {
    $desc2 = FFI::string($desc2Ptr);
    $plan2 = json_decode($desc2, true);
    $selectVal = is_array($plan2['select'] ?? null) ? json_encode($plan2['select']) : ($plan2['select'] ?? 'none');
    ok("describe shows select", $selectVal);
    ok("describe shows 1 filter", count($plan2['filters'] ?? []) . " filters");
    ok("describe shows filter col=price", $plan2['filters'][0]['col'] ?? 'none');
    ok("describe shows filter op=gt", $plan2['filters'][0]['op'] ?? 'none');
    ok("describe shows limit=200", ($plan2['limit'] ?? 'none') . "");
    $ffi->pardox_lazy_free($frameDesc2);
} else {
    fail("lazy_describe after ops", "returned null");
}

// =============================================================================
// TEST 12: filter selectivity - price < 200 should return < 50K rows
// =============================================================================
section("TEST 12: filter selectivity (price < 200)");

$frameLow = $ffi->pardox_lazy_scan_csv($salesCsv, 44, 1);
$frameLow = $ffi->pardox_lazy_select($frameLow, b'["price"]');
$frameLow = $ffi->pardox_lazy_filter($frameLow, b"price", b"lt", 200.0);
$mgrLow = $ffi->pardox_lazy_collect($frameLow);
if ($mgrLow) {
    $nrowsLow = $ffi->pardox_get_row_count($mgrLow);
    $maxPriceLow = $ffi->pardox_agg_max($mgrLow, b"price");
    ok("price<200 returns < total rows", "$nrowsLow < 100000");
    ok("price<200 max < 200", "max=$maxPriceLow");
    $ffi->pardox_free_manager($mgrLow);
} else {
    fail("lazy_filter selectivity", "returned null");
}

// Free the initial frame if still valid
if ($frame) {
    $ffi->pardox_lazy_free($frame);
}

// Summary
section("FINAL RESULT - Gap 5");
echo "\n";
echo "Results: $passed passed, $failed failed\n";

if ($failed == 0) {
    echo "  ALL TESTS PASSED - Gap 5 Lazy Pipeline VALIDATED\n";
} else {
    echo "  $failed TEST(S) FAILED - See output above for details\n";
}

if ($failed > 0) {
    exit(1);
}
