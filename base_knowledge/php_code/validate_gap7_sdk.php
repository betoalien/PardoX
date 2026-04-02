<?php
/**
 * validate_gap7_sdk.php - Gap 7: GPU Compute Beyond Sort (PHP SDK)
 *
 * Validates GPU-accelerated math functions:
 *   Binary: gpu_add, gpu_sub, gpu_mul, gpu_div
 *   Unary:  gpu_sqrt, gpu_exp, gpu_log, gpu_abs
 *
 * Each function tries GPU first; falls back to CPU if GPU is unavailable.
 * Results must match expected math within tolerance (~1e-3 for f32 GPU precision).
 */

require_once __DIR__ . '/vendor/autoload.php';

use PardoX\DataFrame;

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
$csvPath = '_gap7_test.csv';

// Write small deterministic test CSV (16 rows)
$valuesA = [1.0, 4.0, 9.0, 16.0, 25.0, 0.5, 2.0, 8.0,
            0.25, 100.0, 3.0, 7.0, 0.1, 50.0, 0.01, 64.0];
$valuesB = [2.0, 2.0, 3.0, 4.0, 5.0, 0.5, 1.0, 2.0,
            0.5, 10.0, 3.0, 7.0, 0.1, 5.0, 0.01, 8.0];
$valuesNeg = [];
foreach ($valuesA as $i => $v) {
    $valuesNeg[] = ($i % 2 == 0) ? -$v : $v;
}

$fp = fopen($csvPath, 'w');
fwrite($fp, "val_a,val_b,val_neg\n");
for ($i = 0; $i < count($valuesA); $i++) {
    fwrite($fp, "{$valuesA[$i]},{$valuesB[$i]},{$valuesNeg[$i]}\n");
}
fclose($fp);

if (!file_exists($csvPath)) {
    die("Missing required file: $csvPath\n");
}

echo "\n[INIT] Starting PardoX PHP SDK - Gap 7 validation...";

$ffi = \PardoX\Core\FFI::getInstance();
$TOLERANCE = 1e-3;
$NROWS = count($valuesA);

// Helper: get F64 values from a manager
function get_f64_values($mgr, $col, $ffi) {
    $outLen = FFI::new("size_t");
    $ptr = $ffi->pardox_get_f64_buffer($mgr, $col, FFI::addr($outLen));
    if (!$ptr || $outLen->cdata == 0) return null;
    $nrows = $outLen->cdata;
    $arr = FFI::cast("double[$nrows]", $ptr);
    $result = [];
    for ($i = 0; $i < $nrows; $i++) {
        $result[] = $arr[$i];
    }
    return $result;
}

// Load CSV helper
function load_csv($path, $ffi, $cols = null) {
    if ($cols === null) {
        $cols = ["val_a", "val_b", "val_neg"];
    }
    $types = array_fill(0, count($cols), "Float64");
    $schema = json_encode(["column_names" => $cols, "column_types" => $types]);
    $config = json_encode(["delimiter" => ",", "has_header" => true]);
    return $ffi->pardox_load_manager_csv($path, $schema, $config);
}

// =============================================================================
// BINARY OPS
// =============================================================================
section("Binary Operations (GPU)");

// Test 1: GPU ADD
$mgr = load_csv($csvPath, $ffi);
$res = $ffi->pardox_gpu_add($mgr, "val_a", "val_b");
$ffi->pardox_free_manager($mgr);
if ($res) {
    $vals = get_f64_values($res, "gpu_add", $ffi);
    $expected = [];
    for ($i = 0; $i < $NROWS; $i++) {
        $expected[] = $valuesA[$i] + $valuesB[$i];
    }
    $maxDiff = 0;
    for ($i = 0; $i < $NROWS; $i++) {
        $maxDiff = max($maxDiff, abs($vals[$i] - $expected[$i]));
    }
    if ($maxDiff < $TOLERANCE) {
        ok("pardox_gpu_add (A + B)", "max_diff=$maxDiff");
    } else {
        fail("pardox_gpu_add (A + B)", "max_diff=$maxDiff");
    }
    $ffi->pardox_free_manager($res);
} else {
    fail("pardox_gpu_add", "returned null");
}

// Test 2: GPU SUB
$mgr = load_csv($csvPath, $ffi);
$res = $ffi->pardox_gpu_sub($mgr, "val_a", "val_b");
$ffi->pardox_free_manager($mgr);
if ($res) {
    $vals = get_f64_values($res, "gpu_sub", $ffi);
    $expected = [];
    for ($i = 0; $i < $NROWS; $i++) {
        $expected[] = $valuesA[$i] - $valuesB[$i];
    }
    $maxDiff = 0;
    for ($i = 0; $i < $NROWS; $i++) {
        $maxDiff = max($maxDiff, abs($vals[$i] - $expected[$i]));
    }
    if ($maxDiff < $TOLERANCE) {
        ok("pardox_gpu_sub (A - B)", "max_diff=$maxDiff");
    } else {
        fail("pardox_gpu_sub (A - B)", "max_diff=$maxDiff");
    }
    $ffi->pardox_free_manager($res);
} else {
    fail("pardox_gpu_sub", "returned null");
}

// Test 3: GPU MUL
$mgr = load_csv($csvPath, $ffi);
$res = $ffi->pardox_gpu_mul($mgr, "val_a", "val_b");
$ffi->pardox_free_manager($mgr);
if ($res) {
    $vals = get_f64_values($res, "gpu_mul", $ffi);
    $expected = [];
    for ($i = 0; $i < $NROWS; $i++) {
        $expected[] = $valuesA[$i] * $valuesB[$i];
    }
    $maxDiff = 0;
    for ($i = 0; $i < $NROWS; $i++) {
        $maxDiff = max($maxDiff, abs($vals[$i] - $expected[$i]));
    }
    if ($maxDiff < $TOLERANCE) {
        ok("pardox_gpu_mul (A * B)", "max_diff=$maxDiff");
    } else {
        fail("pardox_gpu_mul (A * B)", "max_diff=$maxDiff");
    }
    $ffi->pardox_free_manager($res);
} else {
    fail("pardox_gpu_mul", "returned null");
}

// Test 4: GPU DIV
$mgr = load_csv($csvPath, $ffi);
$res = $ffi->pardox_gpu_div($mgr, "val_a", "val_b");
$ffi->pardox_free_manager($mgr);
if ($res) {
    $vals = get_f64_values($res, "gpu_div", $ffi);
    $expected = [];
    for ($i = 0; $i < $NROWS; $i++) {
        $expected[] = ($valuesB[$i] != 0) ? $valuesA[$i] / $valuesB[$i] : NAN;
    }
    $maxDiff = 0;
    for ($i = 0; $i < $NROWS; $i++) {
        if (!is_nan($vals[$i]) && !is_nan($expected[$i])) {
            $maxDiff = max($maxDiff, abs($vals[$i] - $expected[$i]));
        }
    }
    if ($maxDiff < $TOLERANCE) {
        ok("pardox_gpu_div (A / B)", "max_diff=$maxDiff");
    } else {
        fail("pardox_gpu_div (A / B)", "max_diff=$maxDiff");
    }
    $ffi->pardox_free_manager($res);
} else {
    fail("pardox_gpu_div", "returned null");
}

// =============================================================================
// UNARY OPS
// =============================================================================
section("Unary Operations (GPU)");

// Test 5: GPU SQRT
$mgr = load_csv($csvPath, $ffi);
$res = $ffi->pardox_gpu_sqrt($mgr, "val_a");
$ffi->pardox_free_manager($mgr);
if ($res) {
    $vals = get_f64_values($res, "gpu_sqrt", $ffi);
    $expected = array_map('sqrt', $valuesA);
    $maxDiff = 0;
    for ($i = 0; $i < $NROWS; $i++) {
        $maxDiff = max($maxDiff, abs($vals[$i] - $expected[$i]));
    }
    if ($maxDiff < $TOLERANCE) {
        ok("pardox_gpu_sqrt (sqrt(val_a))", "max_diff=$maxDiff");
    } else {
        fail("pardox_gpu_sqrt (sqrt(val_a))", "max_diff=$maxDiff");
    }
    $ffi->pardox_free_manager($res);
} else {
    fail("pardox_gpu_sqrt", "returned null");
}

// Test 6: GPU EXP (use scaled values to avoid overflow)
$expPath = $here . '/_gap7_exp.csv';
$expValues = array_map(fn($v) => $v * 0.1, $valuesA);
$fp = fopen($expPath, 'w');
fwrite($fp, "val_small\n");
foreach ($expValues as $v) {
    fwrite($fp, "$v\n");
}
fclose($fp);
$mgr = load_csv($expPath, $ffi, ["val_small"]);
$res = $ffi->pardox_gpu_exp($mgr, "val_small");
$ffi->pardox_free_manager($mgr);
if ($res) {
    $vals = get_f64_values($res, "gpu_exp", $ffi);
    $expected = array_map('exp', $expValues);
    $maxDiff = 0;
    for ($i = 0; $i < $NROWS; $i++) {
        if (!is_infinite($vals[$i]) && !is_infinite($expected[$i])) {
            $maxDiff = max($maxDiff, abs($vals[$i] - $expected[$i]));
        }
    }
    if ($maxDiff < $TOLERANCE) {
        ok("pardox_gpu_exp (exp(val_small))", "max_diff=$maxDiff");
    } else {
        fail("pardox_gpu_exp (exp(val_small))", "max_diff=$maxDiff");
    }
    $ffi->pardox_free_manager($res);
} else {
    fail("pardox_gpu_exp", "returned null");
}

// Test 7: GPU LOG
$mgr = load_csv($csvPath, $ffi);
$res = $ffi->pardox_gpu_log($mgr, "val_a");
$ffi->pardox_free_manager($mgr);
if ($res) {
    $vals = get_f64_values($res, "gpu_log", $ffi);
    $expected = array_map('log', $valuesA);
    $maxDiff = 0;
    for ($i = 0; $i < $NROWS; $i++) {
        $maxDiff = max($maxDiff, abs($vals[$i] - $expected[$i]));
    }
    if ($maxDiff < $TOLERANCE) {
        ok("pardox_gpu_log (log(val_a))", "max_diff=$maxDiff");
    } else {
        fail("pardox_gpu_log (log(val_a))", "max_diff=$maxDiff");
    }
    $ffi->pardox_free_manager($res);
} else {
    fail("pardox_gpu_log", "returned null");
}

// Test 8: GPU ABS
$mgr = load_csv($csvPath, $ffi);
$res = $ffi->pardox_gpu_abs($mgr, "val_neg");
$ffi->pardox_free_manager($mgr);
if ($res) {
    $vals = get_f64_values($res, "gpu_abs", $ffi);
    $expected = array_map('abs', $valuesNeg);
    $maxDiff = 0;
    for ($i = 0; $i < $NROWS; $i++) {
        $maxDiff = max($maxDiff, abs($vals[$i] - $expected[$i]));
    }
    if ($maxDiff < $TOLERANCE) {
        ok("pardox_gpu_abs (abs(val_neg))", "max_diff=$maxDiff");
    } else {
        fail("pardox_gpu_abs (abs(val_neg))", "max_diff=$maxDiff");
    }
    // Check no negatives
    $allPositive = true;
    foreach ($vals as $v) {
        if ($v < 0) $allPositive = false;
    }
    if ($allPositive) {
        ok("pardox_gpu_abs - no negatives in output", "min=" . min($vals));
    } else {
        fail("pardox_gpu_abs - has negative values", "min=" . min($vals));
    }
    $ffi->pardox_free_manager($res);
} else {
    fail("pardox_gpu_abs", "returned null");
}

// =============================================================================
// STRUCTURAL CHECKS
// =============================================================================
section("Structural Checks");

// Test 9: Output manager has correct row count
$mgr = load_csv($csvPath, $ffi);
$res = $ffi->pardox_gpu_mul($mgr, "val_a", "val_b");
$ffi->pardox_free_manager($mgr);
if ($res) {
    $nrowsOut = $ffi->pardox_get_row_count($res);
    $ffi->pardox_free_manager($res);
    if ($nrowsOut == $NROWS) {
        ok("Output manager has correct row count", "expected $NROWS, got $nrowsOut");
    } else {
        fail("Output manager row count", "expected $NROWS, got $nrowsOut");
    }
} else {
    fail("Output manager row count", "null result");
}

// Test 10: Input manager is NOT modified
$mgr = load_csv($csvPath, $ffi);
$nrowsBefore = $ffi->pardox_get_row_count($mgr);
$res = $ffi->pardox_gpu_add($mgr, "val_a", "val_b");
$nrowsAfter = $ffi->pardox_get_row_count($mgr);
$ffi->pardox_free_manager($mgr);
if ($res) $ffi->pardox_free_manager($res);
if ($nrowsBefore == $nrowsAfter) {
    ok("Input manager intact after GPU op", "before=$nrowsBefore after=$nrowsAfter");
} else {
    fail("Input manager modified", "before=$nrowsBefore after=$nrowsAfter");
}

// Test 11: Invalid column returns null
$mgr = load_csv($csvPath, $ffi);
$res = $ffi->pardox_gpu_add($mgr, "nonexistent_col", "val_b");
$ffi->pardox_free_manager($mgr);
if (!$res || $res == 0) {
    ok("Invalid column returns null");
} else {
    fail("Invalid column should return null", "ptr=$res");
    $ffi->pardox_free_manager($res);
}

// Test 12: Chained GPU ops (add then sqrt)
$mgr = load_csv($csvPath, $ffi);
$resAdd = $ffi->pardox_gpu_add($mgr, "val_a", "val_b");
$ffi->pardox_free_manager($mgr);
if ($resAdd) {
    $resSqrt = $ffi->pardox_gpu_sqrt($resAdd, "gpu_add");
    $ffi->pardox_free_manager($resAdd);
    if ($resSqrt) {
        $vals = get_f64_values($resSqrt, "gpu_sqrt", $ffi);
        $expected = [];
        for ($i = 0; $i < $NROWS; $i++) {
            $expected[] = sqrt($valuesA[$i] + $valuesB[$i]);
        }
        $maxDiff = 0;
        for ($i = 0; $i < $NROWS; $i++) {
            $maxDiff = max($maxDiff, abs($vals[$i] - $expected[$i]));
        }
        if ($maxDiff < $TOLERANCE) {
            ok("Chained GPU ops (add -> sqrt)", "max_diff=$maxDiff");
        } else {
            fail("Chained GPU ops (add -> sqrt)", "max_diff=$maxDiff");
        }
        $ffi->pardox_free_manager($resSqrt);
    } else {
        fail("Chained GPU ops (add -> sqrt)", "sqrt returned null");
    }
} else {
    fail("Chained GPU ops (add -> sqrt)", "add returned null");
}

// Cleanup temp files
@unlink($csvPath);
@unlink($expPath);

// Summary
section("FINAL RESULT - Gap 7");
echo "\n";
echo "Results: $passed passed, $failed failed\n";

if ($failed == 0) {
    echo "  ALL TESTS PASSED - Gap 7 GPU Compute VALIDATED\n";
} else {
    echo "  $failed TEST(S) FAILED - See output above for details\n";
}

exit($failed > 0 ? 1 : 0);
