<?php
/**
 * validate_gap8_sdk.php - Gap 8: Pivot Table & Melt (PHP SDK)
 *
 * Validates pardox_pivot_table and pardox_melt over:
 *   - CSV-loaded data
 *   - Basic correctness tests for aggregations
 *   - Melt from wide to long format
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

function get_schema($mgr, $ffi) {
    $raw = $ffi->pardox_get_schema_json($mgr);
    if (!$raw) return [];
    return json_decode(FFI::string($raw), true);
}

function nrows($mgr, $ffi) {
    return $ffi->pardox_get_row_count($mgr);
}

function col_names($mgr, $ffi) {
    $schema = get_schema($mgr, $ffi);
    return array_column($schema['columns'] ?? [], 'name');
}

// =============================================================================
// TEST DATA SETUP
// =============================================================================
echo "\n[INIT] Starting PardoX PHP SDK - Gap 8 validation...";

// Synthetic CSV for pivot testing (region x category x amount)
$synthCsv = '_gap8_synth.csv';
$fp = fopen($synthCsv, 'w');
fwrite($fp, "region,category,amount\n");
$data = [
    ["North","A",100.0], ["North","B",200.0], ["North","C",300.0],
    ["South","A",150.0], ["South","B",250.0], ["South","C",350.0],
    ["North","A", 50.0], ["North","B", 80.0], ["North","C",120.0],
    ["South","A", 70.0], ["South","B", 90.0], ["South","C",110.0],
];
foreach ($data as $row) {
    fwrite($fp, implode(",", $row) . "\n");
}
fclose($fp);

// Expected pivot sums (row 0=North, row 1=South; cols A,B,C)
$expectedSum = [
    0 => ["A"=>150.0,"B"=>280.0,"C"=>420.0],
    1 => ["A"=>220.0,"B"=>340.0,"C"=>460.0]
];

$ffi = \PardoX\Core\FFI::getInstance();

// Load CSV helper
function load_csv($path, $ffi) {
    $config = json_encode(["delimiter" => ",", "has_header" => true]);
    return $ffi->pardox_load_manager_csv($path, "{}", $config);
}

// =============================================================================
// Section 1: pivot_table from CSV
// =============================================================================
section("pivot_table (CSV source)");

$synthMgr = load_csv($synthCsv, $ffi);

// Test 1: pivot with sum - non-null
$pivotSum = $ffi->pardox_pivot_table($synthMgr, "region", "category", "amount", "sum");
if ($pivotSum) {
    ok("Test 1 - pivot_table(sum) returns non-null");
} else {
    fail("Test 1 - pivot_table returned null");
}

// Test 2: row count == 2 (one per unique region)
if ($pivotSum) {
    $rc = nrows($pivotSum, $ffi);
    if ($rc == 2) {
        ok("Test 2 - row count == 2", "got $rc");
    } else {
        fail("Test 2 - row count", "expected 2, got $rc");
    }
}

// Test 3: schema has 4 columns (region + A + B + C)
if ($pivotSum) {
    $cols = col_names($pivotSum, $ffi);
    if (count($cols) == 4) {
        ok("Test 3 - schema has 4 columns", implode(", ", $cols));
    } else {
        fail("Test 3 - column count", "expected 4, got " . count($cols));
    }
}

// Test 4: first column is the index column (region)
if ($pivotSum) {
    $cols = col_names($pivotSum, $ffi);
    if ($cols && $cols[0] == "region") {
        ok("Test 4 - first column is 'region'");
    } else {
        fail("Test 4 - first column", "expected 'region', got " . ($cols[0] ?? 'none'));
    }
}

// Test 5: pivot column headers are exactly {"A","B","C"}
if ($pivotSum) {
    $cols = col_names($pivotSum, $ffi);
    $pivotHeaders = array_slice($cols, 1);
    sort($pivotHeaders);
    if ($pivotHeaders == ["A","B","C"]) {
        ok("Test 5 - pivot headers are {A,B,C}");
    } else {
        fail("Test 5 - pivot headers", "got " . implode(",",$pivotHeaders));
    }
}

// Test 6: sum values per cell
if ($pivotSum) {
    $errors = [];
    for ($rowI = 0; $rowI < 2; $rowI++) {
        foreach (["A","B","C"] as $cat) {
            $vals = get_f64_values($pivotSum, $cat, $ffi);
            if ($vals && isset($vals[$rowI])) {
                $exp = $expectedSum[$rowI][$cat];
                if (abs($vals[$rowI] - $exp) > 1e-6) {
                    $errors[] = "row$rowI/$cat: exp=$exp, got=".$vals[$rowI];
                }
            } else {
                $errors[] = "row$rowI/$cat: column not found";
            }
        }
    }
    if (empty($errors)) {
        ok("Test 6 - all SUM cell values match");
    } else {
        fail("Test 6 - SUM values", implode("; ", $errors));
    }
    $ffi->pardox_free_manager($pivotSum);
}

// Test 7: mean aggregation
$pivotMean = $ffi->pardox_pivot_table($synthMgr, "region", "category", "amount", "mean");
if ($pivotMean) {
    $valsA = get_f64_values($pivotMean, "A", $ffi);
    // North/A mean = (100+50)/2 = 75, South/A mean = (150+70)/2 = 110
    if ($valsA && abs($valsA[0] - 75.0) < 1e-6 && abs($valsA[1] - 110.0) < 1e-6) {
        ok("Test 7 - MEAN values correct", "North/A=".$valsA[0].", South/A=".$valsA[1]);
    } else {
        fail("Test 7 - MEAN values", "got ".json_encode($valsA));
    }
    $ffi->pardox_free_manager($pivotMean);
} else {
    fail("Test 7 - pivot mean returned null");
}

// Test 8: count aggregation
$pivotCnt = $ffi->pardox_pivot_table($synthMgr, "region", "category", "amount", "count");
if ($pivotCnt) {
    $valsA = get_f64_values($pivotCnt, "A", $ffi);
    if ($valsA && count($valsA) == 2 && abs($valsA[0]-2.0)<1e-6 && abs($valsA[1]-2.0)<1e-6) {
        ok("Test 8 - COUNT values == 2.0 for each cell");
    } else {
        fail("Test 8 - COUNT values", json_encode($valsA));
    }
    $ffi->pardox_free_manager($pivotCnt);
} else {
    fail("Test 8 - pivot count returned null");
}

// Test 9: min and max
$pivotMin = $ffi->pardox_pivot_table($synthMgr, "region", "category", "amount", "min");
$pivotMax = $ffi->pardox_pivot_table($synthMgr, "region", "category", "amount", "max");
if ($pivotMin && $pivotMax) {
    $minVals = get_f64_values($pivotMin, "A", $ffi);
    $maxVals = get_f64_values($pivotMax, "A", $ffi);
    // North/A min=50, max=100; South/A min=70, max=150
    if ($minVals && $maxVals &&
        abs($minVals[0]-50)<1e-6 && abs($maxVals[0]-100)<1e-6 &&
        abs($minVals[1]-70)<1e-6 && abs($maxVals[1]-150)<1e-6) {
        ok("Test 9 - MIN and MAX values correct");
    } else {
        fail("Test 9 - MIN/MAX values", "min=".json_encode($minVals).", max=".json_encode($maxVals));
    }
    $ffi->pardox_free_manager($pivotMin);
    $ffi->pardox_free_manager($pivotMax);
} else {
    fail("Test 9 - pivot min/max returned null");
}

// Test 10: missing cell filled with 0.0
$asymCsv = $here . '/_gap8_asym.csv';
$fp = fopen($asymCsv, 'w');
fwrite($fp, "region,category,amount\n");
fwrite($fp, "North,A,100.0\n");
fwrite($fp, "North,B,200.0\n");
fwrite($fp, "South,A,50.0\n");  // South/B intentionally missing
fclose($fp);

$asymMgr = load_csv($asymCsv, $ffi);
$pivotAsym = $ffi->pardox_pivot_table($asymMgr, "region", "category", "amount", "sum");
if ($pivotAsym) {
    $bVals = get_f64_values($pivotAsym, "B", $ffi);
    // South is row 1; South/B should be 0.0
    if ($bVals && count($bVals) == 2 && abs($bVals[1] - 0.0) < 1e-9) {
        ok("Test 10 - missing cell filled with 0.0 (South/B=0.0)");
    } else {
        fail("Test 10 - missing cell", "B col=".json_encode($bVals));
    }
    $ffi->pardox_free_manager($pivotAsym);
} else {
    fail("Test 10 - asymmetric pivot returned null");
}
$ffi->pardox_free_manager($asymMgr);

// Test 11: invalid columns_col -> null
$pivotBad = $ffi->pardox_pivot_table($synthMgr, "region", "__no_such_col__", "amount", "sum");
if (!$pivotBad || $pivotBad == 0) {
    ok("Test 11 - invalid columns_col -> null returned");
} else {
    fail("Test 11 - should have been null");
    $ffi->pardox_free_manager($pivotBad);
}

// Test 12: unknown agg_func -> null
$pivotBad2 = $ffi->pardox_pivot_table($synthMgr, "region", "category", "amount", "median");
if (!$pivotBad2 || $pivotBad2 == 0) {
    ok("Test 12 - unknown agg_func -> null returned");
} else {
    fail("Test 12 - should have been null");
    $ffi->pardox_free_manager($pivotBad2);
}

$ffi->pardox_free_manager($synthMgr);

// =============================================================================
// Section 2: melt from CSV
// =============================================================================
section("melt (CSV source)");

// Wide CSV: id(Int64), q1..q4 (Float64)
$wideCsv = $here . '/_gap8_wide.csv';
$wideData = [
    [1, 10.0, 20.0, 30.0, 40.0],
    [2, 50.0, 60.0, 70.0, 80.0],
    [3, 90.0,100.0,110.0,120.0],
];
$fp = fopen($wideCsv, 'w');
fwrite($fp, "id,q1,q2,q3,q4\n");
foreach ($wideData as $row) {
    fwrite($fp, implode(",", $row) . "\n");
}
fclose($fp);

$wideMgr = load_csv($wideCsv, $ffi);
$idVars = json_encode(["id"]);
$valVars = json_encode(["q1","q2","q3","q4"]);

$melt1 = $ffi->pardox_melt($wideMgr, $idVars, $valVars, "quarter", "revenue");

// Test 13: returns non-null
if ($melt1) {
    ok("Test 13 - pardox_melt returns non-null");
} else {
    fail("Test 13 - melt returned null");
}

// Test 14: row count = 3 × 4 = 12
if ($melt1) {
    $meltRc = nrows($melt1, $ffi);
    if ($meltRc == 12) {
        ok("Test 14 - row count == 12 (3 orig × 4 value_vars)", "got $meltRc");
    } else {
        fail("Test 14 - row count", "expected 12, got $meltRc");
    }
}

// Test 15: schema has 3 columns
if ($melt1) {
    $meltCols = col_names($melt1, $ffi);
    if (count($meltCols) == 3 && in_array("id", $meltCols) && in_array("quarter", $meltCols) && in_array("revenue", $meltCols)) {
        ok("Test 15 - schema has {id, quarter, revenue}");
    } else {
        fail("Test 15 - schema", "got ".json_encode($meltCols));
    }
}

// Test 16: column types - quarter=Utf8, revenue=Float64
if ($melt1) {
    $schema = get_schema($melt1, $ffi);
    $typeMap = [];
    foreach ($schema['columns'] as $c) {
        $typeMap[$c['name']] = $c['type'];
    }
    if (isset($typeMap['quarter']) && $typeMap['quarter'] == 'Utf8' &&
        isset($typeMap['revenue']) && $typeMap['revenue'] == 'Float64') {
        ok("Test 16 - 'quarter' is Utf8, 'revenue' is Float64");
    } else {
        fail("Test 16 - column types", json_encode($typeMap));
    }
}

// Test 17: revenue Float64 values match original wide data
if ($melt1) {
    $revVals = get_f64_values($melt1, "revenue", $ffi);
    $expectedRev = [10.0,20.0,30.0,40.0, 50.0,60.0,70.0,80.0, 90.0,100.0,110.0,120.0];
    $match = true;
    if (count($revVals) == 12) {
        for ($i = 0; $i < 12; $i++) {
            if (abs($revVals[$i] - $expectedRev[$i]) > 1e-6) {
                $match = false;
                break;
            }
        }
    } else {
        $match = false;
    }
    if ($match) {
        ok("Test 17 - revenue values match original data");
    } else {
        fail("Test 17 - revenue values", "expected ".json_encode(array_slice($expectedRev,0,4)).", got ".json_encode(array_slice($revVals,0,4)));
    }
    $ffi->pardox_free_manager($melt1);
}

// Test 18: multiple id_vars
$multiCsv = $here . '/_gap8_multi.csv';
$fp = fopen($multiCsv, 'w');
fwrite($fp, "store,year,jan,feb,mar\n");
fwrite($fp, "StoreA,2024,100.0,110.0,120.0\n");
fwrite($fp, "StoreB,2024,200.0,210.0,220.0\n");
fclose($fp);

$midMgr = load_csv($multiCsv, $ffi);
$midId = json_encode(["store","year"]);
$midVal = json_encode(["jan","feb","mar"]);
$melt2 = $ffi->pardox_melt($midMgr, $midId, $midVal, "month", "sales");
if ($melt2) {
    $m2rc = nrows($melt2, $ffi);
    $m2cols = col_names($melt2, $ffi);
    $sales = get_f64_values($melt2, "sales", $ffi);
    // Expected: 6 rows, cols: store,year,month,sales
    $expSales = [100.0,110.0,120.0, 200.0,210.0,220.0];
    $salesMatch = true;
    if (count($sales) == 6) {
        for ($i = 0; $i < 6; $i++) {
            if (abs($sales[$i] - $expSales[$i]) > 1e-6) {
                $salesMatch = false;
                break;
            }
        }
    } else {
        $salesMatch = false;
    }
    if ($m2rc == 6 && count($m2cols) == 4 && $salesMatch) {
        ok("Test 18 - melt with multiple id_vars (store+year)", "rows=$m2rc, cols=".count($m2cols));
    } else {
        fail("Test 18 - multi-id melt", "rows=$m2rc, cols=".count($m2cols).", sales=".json_encode($sales));
    }
    $ffi->pardox_free_manager($melt2);
} else {
    fail("Test 18 - multi-id melt returned null");
}
$ffi->pardox_free_manager($midMgr);
$ffi->pardox_free_manager($wideMgr);

// Test 19: invalid value_var -> null
$wideMgr2 = load_csv($wideCsv, $ffi);
$meltBad = $ffi->pardox_melt($wideMgr2, json_encode(["id"]), json_encode(["__no_such__"]), "var", "val");
if (!$meltBad || $meltBad == 0) {
    ok("Test 19 - invalid value_var -> null returned");
} else {
    fail("Test 19 - should have been null");
    $ffi->pardox_free_manager($meltBad);
}
$ffi->pardox_free_manager($wideMgr2);

// Cleanup temp files
@unlink($synthCsv);
@unlink($asymCsv);
@unlink($wideCsv);
@unlink($here . '/_gap8_multi.csv');

// Summary
section("FINAL RESULT - Gap 8");
echo "\n";
echo "Results: $passed passed, $failed failed\n";

if ($failed == 0) {
    echo "  ALL TESTS PASSED - Gap 8 Pivot & Melt VALIDATED\n";
} else {
    echo "  $failed TEST(S) FAILED - See output above for details\n";
}

exit($failed > 0 ? 1 : 0);
