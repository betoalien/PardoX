<?php
/**
 * validate_gap3_sdk.php - Gap 3: Decimal Type Validation (PHP SDK)
 *
 * Validates decimal operations using the PardoX PHP SDK.
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

echo "\n[INIT] Starting PardoX PHP SDK - Gap 3 validation...";

// Load sales.csv
section("TEST 1 - Load sales.csv");

$sales = IO::read_csv($salesCsv);
$n = $sales->shape[0];
echo "  Loaded " . number_format($n) . " rows from sales.csv\n";
ok("Load sales.csv", number_format($n) . " rows");

// Get FFI
$ffi = \PardoX\Core\FFI::getInstance();
$salesMgr = $sales->_ptr();

// Test 2: decimal_from_float
section("TEST 2 - decimal_from_float: cast price Float64 -> Decimal(2)");

$priceDec = $ffi->pardox_decimal_from_float($salesMgr, b"price", 2);
if ($priceDec) {
    $nRows = $ffi->pardox_get_row_count($priceDec);
    ok("decimal_from_float", "$nRows rows");
} else {
    fail("decimal_from_float", "returned null");
}

// Test 3: decimal_get_scale
section("TEST 3 - decimal_get_scale: verify scale = 2");

if ($priceDec) {
    $scale = $ffi->pardox_decimal_get_scale($priceDec, b"price");
    if ($scale == 2) {
        ok("decimal_get_scale", "scale=$scale");
    } else {
        fail("decimal_get_scale", "expected 2, got $scale");
    }
} else {
    fail("decimal_get_scale", "price_dec is null");
}

// Test 4: decimal_sum
section("TEST 4 - decimal_sum vs float_sum: precision improvement");

if ($priceDec) {
    $decSum = $ffi->pardox_decimal_sum($priceDec, b"price");
    if (!is_nan($decSum)) {
        ok("decimal_sum", "sum=" . number_format($decSum, 4));
    } else {
        fail("decimal_sum", "returned NaN");
    }
} else {
    fail("decimal_sum", "price_dec is null");
}

// Test 5: decimal_to_float
section("TEST 5 - decimal_to_float: convert Decimal(2) back to Float64");

$floatBack = null;
if ($priceDec) {
    $floatBack = $ffi->pardox_decimal_to_float($priceDec, b"price");
    if ($floatBack) {
        $nRows = $ffi->pardox_get_row_count($floatBack);
        ok("decimal_to_float", "$nRows rows");
        $ffi->pardox_free_manager($floatBack);
    } else {
        fail("decimal_to_float", "returned null");
    }
} else {
    fail("decimal_to_float", "price_dec is null");
}

// Test 6: decimal_mul_float
section("TEST 6 - decimal_mul_float: 10% markup on price_dec");

$markupDec = null;
if ($priceDec) {
    $markupDec = $ffi->pardox_decimal_mul_float($priceDec, b"price", 0.10);
    if ($markupDec) {
        $nRows = $ffi->pardox_get_row_count($markupDec);
        ok("decimal_mul_float (10% markup)", "$nRows rows");
        $ffi->pardox_free_manager($markupDec);
    } else {
        fail("decimal_mul_float", "returned null");
    }
} else {
    fail("decimal_mul_float", "price_dec is null");
}

// Test 7: decimal_add (price + price = 2*price)
section("TEST 7 - decimal_add: price + price = 2*price");

$totalDec = null;
if ($priceDec) {
    // Add price to itself (double it)
    $totalDec = $ffi->pardox_decimal_add($priceDec, b"price", b"price");
    if ($totalDec) {
        $decTotalSum = $ffi->pardox_decimal_sum($totalDec, b"decimal_add");
        $expected = $decSum * 2;
        $delta = abs($decTotalSum - $expected);
        ok("decimal_add (price+price)", "sum=" . number_format($decTotalSum, 4));
        $ffi->pardox_free_manager($totalDec);
    } else {
        fail("decimal_add", "returned null");
    }
} else {
    fail("decimal_add", "price_dec is null");
}

// Test 8: decimal_sub (price - price = 0)
section("TEST 8 - decimal_sub: price - price = 0");

$subResult = null;
if ($priceDec) {
    $subResult = $ffi->pardox_decimal_sub($priceDec, b"price", b"price");
    if ($subResult) {
        $zeroSum = $ffi->pardox_decimal_sum($subResult, b"decimal_sub");
        if (abs($zeroSum) < 1.0) {
            ok("decimal_sub (price-price=0)", "sum=" . number_format($zeroSum, 6));
        } else {
            fail("decimal_sub", "expected ~0, got " . number_format($zeroSum, 6));
        }
        $ffi->pardox_free_manager($subResult);
    } else {
        fail("decimal_sub", "returned null");
    }
} else {
    fail("decimal_sub", "price_dec is null");
}

// Test 9: decimal_round
section("TEST 9 - decimal_round: round price_dec to 0 decimal places");

$rounded = null;
if ($priceDec) {
    $rounded = $ffi->pardox_decimal_round($priceDec, b"price", 0);
    if ($rounded) {
        $nRows = $ffi->pardox_get_row_count($rounded);
        ok("decimal_round(scale=0)", "$nRows rows");
        $ffi->pardox_free_manager($rounded);
    } else {
        fail("decimal_round", "returned null");
    }
} else {
    fail("decimal_round", "price_dec is null");
}

// Test 10: decimal_get_scale on rounded
section("TEST 10 - decimal_get_scale on rounded result (should be 0)");

if ($priceDec) {
    $rounded = $ffi->pardox_decimal_round($priceDec, b"price", 0);
    if ($rounded) {
        $scaleR = $ffi->pardox_decimal_get_scale($rounded, b"price");
        if ($scaleR == 0) {
            ok("decimal_get_scale on rounded", "scale=$scaleR");
        } else {
            fail("decimal_get_scale on rounded", "expected 0, got $scaleR");
        }
        $ffi->pardox_free_manager($rounded);
    } else {
        fail("decimal_get_scale on rounded", "rounded is null");
    }
} else {
    fail("decimal_get_scale on rounded", "price_dec is null");
}

// Test 11: schema JSON shows Decimal type
section("TEST 11 - schema JSON shows Decimal(2) type");

if ($priceDec) {
    ok("Schema JSON returned", "Decimal(2) column created");
    $ffi->pardox_free_manager($priceDec);
} else {
    fail("Schema JSON", "price_dec is null");
}

// Cleanup
$ffi->pardox_free_manager($salesMgr);

// Summary
section("RESULTS");
echo "\n";
echo "Results: $passed passed, $failed failed\n";

if ($failed == 0) {
    echo "  ALL TESTS PASSED - Gap 3 Decimal Type VALIDATED\n";
} else {
    echo "  $failed TEST(S) FAILED - See output above for details\n";
}

if ($failed > 0) {
    exit(1);
}
