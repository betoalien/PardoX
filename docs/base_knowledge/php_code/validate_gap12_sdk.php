<?php
/**
 * validate_gap12_sdk.php - Gap 12: Universal Manager Loader (PRDX -> HyperBlockManager)
 *
 * Tests pardox_load_manager_prdx, which converts a .prdx binary file directly
 * into a HyperBlockManager, enabling all existing Gap 1–7 compute functions to
 * operate on PRDX data without going through CSV.
 *
 * Dataset: ventas_consolidado.prdx (50M rows, 1.33 GB)
 * Tests use limit_rows=10000 for speed.
 */

require_once __DIR__ . '/vendor/autoload.php';

use PardoX\Core\FFI;
use PardoX\Core\Lib;

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
    echo "  [OK]   $name" . ($detail ? " - $detail" : "") . "\n";
}

function fail(string $name, string $detail = ""): void {
    global $failed;
    $failed++;
    echo "  [FAIL] $name" . ($detail ? " - $detail" : "") . "\n";
}

// Get paths
$prdxPath = 'ventas_consolidado.prdx';

if (!file_exists($prdxPath)) {
    die("Missing required file: $prdxPath\n");
}

echo "\n[INIT] Starting PardoX PHP SDK - Gap 12 validation...\n";

$ffi = FFI::getInstance();

$prdxSizeGB = round(filesize($prdxPath) / 1e9, 2);
echo "[INFO] PRDX file: $prdxPath ($prdxSizeGB GB)\n\n";

$LIMIT = 10000;

section("Gap 12: Universal Manager Loader (PRDX -> HyperBlockManager)");
echo "Dataset: " . basename($prdxPath) . "  |  limit_rows=$LIMIT\n";

// =============================================================================
// Test 1: load_manager_prdx returns non-null
// =============================================================================
section("Test 1 - load_manager_prdx returns non-null");

$mgr = $ffi->pardox_load_manager_prdx($prdxPath, $LIMIT);
if ($mgr) {
    ok('load_manager_prdx returns non-null');
} else {
    fail('load_manager_prdx returned null');
    exit(1);
}

// =============================================================================
// Test 2: row count
// =============================================================================
section("Test 2 - row count == $LIMIT");

$nrows = $ffi->pardox_get_row_count($mgr);
if ($nrows === $LIMIT) {
    ok("row count == $LIMIT", "got $nrows");
} else {
    fail("row count mismatch", "expected $LIMIT, got $nrows");
}

// =============================================================================
// Test 3: schema has all expected columns
// =============================================================================
section('Test 3 - schema has all expected columns');

$schemaJson = $ffi->pardox_get_schema_json($mgr);
$schema = json_decode(\FFI::string($schemaJson), true);
$cols = array_map(fn($c) => $c['name'], $schema['columns']);
$expectedCols = ['transaction_id', 'client_id', 'date_time', 'entity', 'category', 'client_segment', 'amount', 'tax_rate'];
$missingCols = array_diff($expectedCols, $cols);

if (count($missingCols) === 0) {
    ok("schema has all " . count($expectedCols) . " expected columns", count($cols) . " columns OK");
} else {
    fail("missing columns", implode(', ', $missingCols));
}

// =============================================================================
// Test 4: GroupBy entity -> count (Gap 1 via PRDX)
// =============================================================================
section('Test 4 - GroupBy entity -> count (Gap 1 via PRDX)');

$grp = $ffi->pardox_groupby_agg($mgr, json_encode(['entity']), json_encode(['transaction_id' => 'count']));
if ($grp) {
    $grpRows = $ffi->pardox_get_row_count($grp);
    ok("GroupBy entity -> count (Gap 1 via PRDX)", "$grpRows distinct entities");
    $ffi->pardox_free_manager($grp);
} else {
    fail('GroupBy returned null');
}

// =============================================================================
// Test 5: GroupBy category -> sum(amount) (Gap 1 via PRDX)
// =============================================================================
section('Test 5 - GroupBy category -> sum(amount) (Gap 1 via PRDX)');

$grp2 = $ffi->pardox_groupby_agg($mgr, json_encode(['category']), json_encode(['amount' => 'sum']));
if ($grp2) {
    $grp2Rows = $ffi->pardox_get_row_count($grp2);
    $outLen = \FFI::new("size_t");
    $ptr = $ffi->pardox_get_f64_buffer($grp2, "amount", \FFI::addr($outLen));
    if ($ptr && $outLen->cdata > 0) {
        $nrows2 = $outLen->cdata;
        $vals = \FFI::cast("double[$nrows2]", $ptr);
        $sample = [];
        for ($i = 0; $i < min(3, $nrows2); $i++) {
            $sample[] = round($vals[$i], 2);
        }
        ok("GroupBy category -> sum(amount) (Gap 1 via PRDX)", "$grp2Rows categories, sums sample=[" . implode(', ', $sample) . "]");
    } else {
        fail('GroupBy sum values are null');
    }
    $ffi->pardox_free_manager($grp2);
} else {
    fail('GroupBy category returned null');
}

// =============================================================================
// Test 6: str_upper on entity (Gap 2 via PRDX)
// =============================================================================
section('Test 6 - str_upper on entity (Gap 2 via PRDX)');

$upper = $ffi->pardox_str_upper($mgr, 'entity');
if ($upper) {
    $upperRows = $ffi->pardox_get_row_count($upper);
    ok("str_upper on entity (Gap 2 via PRDX)", "$upperRows rows");
    $ffi->pardox_free_manager($upper);
} else {
    fail('str_upper returned null');
}

// =============================================================================
// Test 7: str_len on entity (Gap 2 via PRDX)
// =============================================================================
section('Test 7 - str_len on entity (Gap 2 via PRDX)');

$lens = $ffi->pardox_str_len($mgr, 'entity');
if ($lens) {
    $lenRows = $ffi->pardox_get_row_count($lens);
    ok("str_len on entity (Gap 2 via PRDX)", "$lenRows rows");
    $ffi->pardox_free_manager($lens);
} else {
    fail('str_len returned null');
}

// =============================================================================
// Test 8: gpu_add amount + tax_rate (Gap 7 via PRDX)
// =============================================================================
section('Test 8 - gpu_add amount + tax_rate (Gap 7 via PRDX)');

$added = $ffi->pardox_gpu_add($mgr, 'amount', 'tax_rate');
if ($added) {
    $outLen = \FFI::new("size_t");
    $ptr = $ffi->pardox_get_f64_buffer($added, "gpu_add", \FFI::addr($outLen));
    if ($ptr && $outLen->cdata > 0) {
        $n = $outLen->cdata;
        $vals = \FFI::cast("double[$n]", $ptr);
        $sample = [];
        for ($i = 0; $i < min(3, $n); $i++) {
            $sample[] = round($vals[$i], 4);
        }
        ok("gpu_add amount + tax_rate (Gap 7 via PRDX)", "$n values, sample=[" . implode(', ', $sample) . "]");
    }
    $ffi->pardox_free_manager($added);
} else {
    fail('gpu_add returned null');
}

// =============================================================================
// Test 9: gpu_mul amount * tax_rate (Gap 7 via PRDX)
// =============================================================================
section('Test 9 - gpu_mul amount * tax_rate (Gap 7 via PRDX)');

$muld = $ffi->pardox_gpu_mul($mgr, 'amount', 'tax_rate');
if ($muld) {
    $outLen = \FFI::new("size_t");
    $ptr = $ffi->pardox_get_f64_buffer($muld, "gpu_mul", \FFI::addr($outLen));
    if ($ptr && $outLen->cdata > 0) {
        $n = $outLen->cdata;
        $vals = \FFI::cast("double[$n]", $ptr);
        $sample = [];
        for ($i = 0; $i < min(3, $n); $i++) {
            $sample[] = round($vals[$i], 4);
        }
        ok("gpu_mul amount * tax_rate (Gap 7 via PRDX)", "$n values, sample=[" . implode(', ', $sample) . "]");
    }
    $ffi->pardox_free_manager($muld);
} else {
    fail('gpu_mul returned null');
}

// =============================================================================
// Test 10: invalid column -> null returned
// =============================================================================
section('Test 10 - invalid column -> null returned');

$badCol = $ffi->pardox_str_upper($mgr, '__nonexistent_col__');
if (!$badCol) {
    ok('invalid column -> null returned', 'correctly returned null');
} else {
    fail('invalid column should return null');
    $ffi->pardox_free_manager($badCol);
}

// =============================================================================
// Test 11: chained str_upper preserves row count
// =============================================================================
section('Test 11 - chained str_upper preserves row count');

$chained = $ffi->pardox_str_upper($mgr, 'entity');
if ($chained) {
    $chainedRows = $ffi->pardox_get_row_count($chained);
    if ($chainedRows === $LIMIT) {
        ok("chained str_upper preserves row count", "rows=$chainedRows");
    } else {
        fail("chained row count mismatch", "$chainedRows != $LIMIT");
    }
    $ffi->pardox_free_manager($chained);
} else {
    fail('chained str_upper returned null');
}

// =============================================================================
// Test 12: invalid path -> null returned
// =============================================================================
section('Test 12 - invalid path -> null returned');

$badPath = $ffi->pardox_load_manager_prdx('nonexistent.prdx', 100);
if (!$badPath) {
    ok('invalid path -> null returned', 'correctly returned null');
} else {
    fail('invalid path should return null');
    $ffi->pardox_free_manager($badPath);
}

// =============================================================================
// Cleanup
// =============================================================================
$ffi->pardox_free_manager($mgr);

// Summary
section('FINAL RESULT - Gap 12');
echo "\n";
echo "Results: $passed passed, $failed failed\n";

if ($failed == 0) {
    echo "  ALL TESTS PASSED - Gap 12 PRDX Loader VALIDATED\n";
} else {
    echo "  $failed TEST(S) FAILED - See output above for details\n";
}

exit($failed > 0 ? 1 : 0);