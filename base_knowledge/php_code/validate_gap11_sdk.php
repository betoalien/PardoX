<?php
/**
 * validate_gap11_sdk.php - Gap 11: Spill to Disk / Out-of-Core (PHP SDK)
 *
 * Tests pardox_spill_to_disk, pardox_spill_from_disk, pardox_chunked_groupby,
 * pardox_external_sort, pardox_memory_usage using:
 *   - Spill/restore round-trip with data integrity verification
 *   - Chunked GroupBy with disk spill vs in-memory consistency
 *   - External merge sort with disk spill vs in-memory consistency
 *   - Memory usage reporting
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
$salesCsv = 'sales.csv';

if (!file_exists($salesCsv)) {
    die("Missing required file: $salesCsv\n");
}

$ffi = \PardoX\Core\FFI::getInstance();

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

function nrows($mgr, $ffi) {
    return $ffi->pardox_get_row_count($mgr);
}

function col_names($mgr, $ffi) {
    $raw = $ffi->pardox_get_schema_json($mgr);
    if (!$raw) return [];
    $schema = json_decode(FFI::string($raw), true);
    return array_column($schema['columns'] ?? [], 'name');
}

function get_records($mgr, $ffi) {
    $raw = $ffi->pardox_to_json_records($mgr);
    if (!$raw) return [];
    return json_decode(FFI::string($raw), true);
}

function load_csv($path, $ffi) {
    $config = json_encode(["delimiter" => ",", "has_header" => true]);
    return $ffi->pardox_load_manager_csv($path, "{}", $config);
}

// =============================================================================
echo "\n[INIT] Starting PardoX PHP SDK - Gap 11 validation...";

// Load sales.csv (100K rows)
section("SETUP: Load sales.csv (100K rows)");
$mgrSales = load_csv($salesCsv, $ffi);
echo "  sales: " . number_format(nrows($mgrSales, $ffi)) . " rows, cols: " . json_encode(col_names($mgrSales, $ffi)) . "\n";

// =============================================================================
// PART 1: SPILL TO DISK / RESTORE FROM DISK
// =============================================================================
section("TEST 1 - spill_to_disk: serialize to temp file");
$spillPath = 'pardox_test_spill.pxs';
$bytesWritten = $ffi->pardox_spill_to_disk($mgrSales, $spillPath);
if ($bytesWritten > 0) {
    ok("spill_to_disk -> $bytesWritten bytes written to $spillPath");
} else {
    fail("spill_to_disk returned $bytesWritten");
}

section("TEST 2 - spill file exists on disk");
if (file_exists($spillPath)) {
    $fileSize = filesize($spillPath);
    ok("spill file exists: $fileSize bytes");
} else {
    fail("spill file not found on disk");
}

section("TEST 3 - spill_from_disk: restore from temp file");
$restored = $ffi->pardox_spill_from_disk($spillPath);
if ($restored) {
    ok("spill_from_disk -> restored manager, " . number_format(nrows($restored, $ffi)) . " rows");
} else {
    fail("spill_from_disk returned null");
}

section("TEST 4 - round-trip: row count preserved");
if ($restored) {
    $orig = nrows($mgrSales, $ffi);
    $rest = nrows($restored, $ffi);
    if ($orig == $rest) {
        ok("row count preserved: " . number_format($orig) . " == " . number_format($rest));
    } else {
        fail("row count mismatch: " . number_format($orig) . " != " . number_format($rest));
    }
}

section("TEST 5 - round-trip: schema preserved");
if ($restored) {
    $origCols = col_names($mgrSales, $ffi);
    $restCols = col_names($restored, $ffi);
    if ($origCols == $restCols) {
        ok("schema preserved: ".json_encode($origCols));
    } else {
        fail("schema mismatch: ".json_encode($origCols)." != ".json_encode($restCols));
    }
}

section("TEST 6 - round-trip: data integrity (price column)");
if ($restored) {
    $origPrices = get_f64_values($mgrSales, "price", $ffi);
    $restPrices = get_f64_values($restored, "price", $ffi);
    if (count($origPrices) == count($restPrices)) {
        $maxDiff = 0;
        $checkCount = min(1000, count($origPrices));
        for ($i = 0; $i < $checkCount; $i++) {
            $maxDiff = max($maxDiff, abs($origPrices[$i] - $restPrices[$i]));
        }
        if ($maxDiff < 1e-10) {
            ok("price data identical (max_diff=$maxDiff, checked $checkCount rows)");
        } else {
            fail("price data mismatch: max_diff=$maxDiff");
        }
    } else {
        fail("price column length mismatch");
    }
}

if ($restored) {
    $ffi->pardox_free_manager($restored);
}

// Cleanup spill file
if (file_exists($spillPath)) {
    @unlink($spillPath);
}

section("TEST 7 - spill_to_disk: null pointer returns error");
$result = $ffi->pardox_spill_to_disk(null, 'test.pxs');
if ($result < 0) {
    ok("null pointer correctly returns error: $result");
} else {
    fail("null pointer should return negative, got $result");
}

section("TEST 8 - spill_from_disk: nonexistent file returns null");
$bad = $ffi->pardox_spill_from_disk('nonexistent_pardox_file.pxs');
if ($bad === null || $bad == 0) {
    ok("nonexistent file correctly returns null");
} else {
    fail("nonexistent file should return null");
    $ffi->pardox_free_manager($bad);
}

// =============================================================================
// PART 2: CHUNKED GROUPBY
// =============================================================================
section("TEST 9 - chunked_groupby: sum by category (chunk=20000)");
$aggJson = json_encode(["price" => "sum"]);
$chunked = $ffi->pardox_chunked_groupby($mgrSales, "category", $aggJson, 20000);
if ($chunked) {
    ok("chunked_groupby -> " . nrows($chunked, $ffi) . " groups");
} else {
    fail("chunked_groupby returned null");
}

section("TEST 10 - chunked_groupby: compare with in-memory groupby");
// In-memory groupby for reference
$inmem = $ffi->pardox_groupby_agg($mgrSales, json_encode(["category"]), json_encode(["price" => "sum"]));
if ($chunked && $inmem) {
    $chunkedGroups = nrows($chunked, $ffi);
    $inmemGroups = nrows($inmem, $ffi);
    if ($chunkedGroups == $inmemGroups) {
        ok("same number of groups: $chunkedGroups");
    } else {
        fail("group count mismatch: chunked=$chunkedGroups, inmem=$inmemGroups");
    }
} else {
    fail("one of chunked/inmem returned null");
}

section("TEST 11 - chunked_groupby: sum values match in-memory");
if ($chunked && $inmem) {
    $chunkedRecs = get_records($chunked, $ffi);
    $inmemRecs = get_records($inmem, $ffi);

    $chunkedMap = [];
    foreach ($chunkedRecs as $r) {
        $chunkedMap[$r["category"]] = $r["price"];
    }
    $inmemMap = [];
    foreach ($inmemRecs as $r) {
        $inmemMap[$r["category"]] = $r["price"];
    }

    $allMatch = true;
    foreach ($inmemMap as $cat => $val) {
        if (isset($chunkedMap[$cat])) {
            $diff = abs(floatval($chunkedMap[$cat]) - floatval($val));
            if ($diff > 1.0) {
                $allMatch = false;
                break;
            }
        }
    }
    if ($allMatch) {
        ok("sum values match across ".count($inmemMap)." categories (tolerance < 1.0)");
    } else {
        fail("sum values mismatch");
    }
}

if ($chunked) $ffi->pardox_free_manager($chunked);
if ($inmem) $ffi->pardox_free_manager($inmem);

section("TEST 12 - chunked_groupby: min aggregation");
$aggMin = json_encode(["price" => "min"]);
$chunkedMin = $ffi->pardox_chunked_groupby($mgrSales, "category", $aggMin, 25000);
$inmemMin = $ffi->pardox_groupby_agg($mgrSales, json_encode(["category"]), $aggMin);
if ($chunkedMin && $inmemMin) {
    $cRecs = get_records($chunkedMin, $ffi);
    $iRecs = get_records($inmemMin, $ffi);
    $cMap = []; foreach ($cRecs as $r) { $cMap[$r["category"]] = floatval($r["price"]); }
    $iMap = []; foreach ($iRecs as $r) { $iMap[$r["category"]] = floatval($r["price"]); }
    $allMatch = true;
    foreach ($iMap as $cat => $val) {
        if (isset($cMap[$cat]) && abs($cMap[$cat] - $val) > 0.01) {
            $allMatch = false;
            break;
        }
    }
    if ($allMatch) {
        ok("min aggregation matches across ".count($iMap)." categories");
    } else {
        fail("min aggregation mismatch");
    }
    $ffi->pardox_free_manager($chunkedMin);
    $ffi->pardox_free_manager($inmemMin);
} else {
    fail("chunked_groupby min returned null");
}

section("TEST 13 - chunked_groupby: max aggregation");
$aggMax = json_encode(["price" => "max"]);
$chunkedMax = $ffi->pardox_chunked_groupby($mgrSales, "category", $aggMax, 30000);
$inmemMax = $ffi->pardox_groupby_agg($mgrSales, json_encode(["category"]), $aggMax);
if ($chunkedMax && $inmemMax) {
    $cRecs = get_records($chunkedMax, $ffi);
    $iRecs = get_records($inmemMax, $ffi);
    $cMap = []; foreach ($cRecs as $r) { $cMap[$r["category"]] = floatval($r["price"]); }
    $iMap = []; foreach ($iRecs as $r) { $iMap[$r["category"]] = floatval($r["price"]); }
    $allMatch = true;
    foreach ($iMap as $cat => $val) {
        if (isset($cMap[$cat]) && abs($cMap[$cat] - $val) > 0.01) {
            $allMatch = false;
            break;
        }
    }
    if ($allMatch) {
        ok("max aggregation matches across ".count($iMap)." categories");
    } else {
        fail("max aggregation mismatch");
    }
    $ffi->pardox_free_manager($chunkedMax);
    $ffi->pardox_free_manager($inmemMax);
} else {
    fail("chunked_groupby max returned null");
}

section("TEST 14 - chunked_groupby: count aggregation");
$aggCnt = json_encode(["price" => "count"]);
$chunkedCnt = $ffi->pardox_chunked_groupby($mgrSales, "category", $aggCnt, 15000);
$inmemCnt = $ffi->pardox_groupby_agg($mgrSales, json_encode(["category"]), $aggCnt);
if ($chunkedCnt && $inmemCnt) {
    $cRecs = get_records($chunkedCnt, $ffi);
    $iRecs = get_records($inmemCnt, $ffi);
    $cMap = []; foreach ($cRecs as $r) { $cMap[$r["category"]] = floatval($r["price"]); }
    $iMap = []; foreach ($iRecs as $r) { $iMap[$r["category"]] = floatval($r["price"]); }
    $allMatch = true;
    foreach ($iMap as $cat => $val) {
        if (isset($cMap[$cat]) && abs($cMap[$cat] - $val) > 0.01) {
            $allMatch = false;
            break;
        }
    }
    if ($allMatch) {
        ok("count aggregation matches across ".count($iMap)." categories");
    } else {
        fail("count aggregation mismatch");
    }
    $ffi->pardox_free_manager($chunkedCnt);
    $ffi->pardox_free_manager($inmemCnt);
} else {
    fail("chunked_groupby count returned null");
}

section("TEST 15 - chunked_groupby: small chunk size (1000 rows)");
$aggSum = json_encode(["price" => "sum"]);
$chunkedSmall = $ffi->pardox_chunked_groupby($mgrSales, "category", $aggSum, 1000);
$inmemSum = $ffi->pardox_groupby_agg($mgrSales, json_encode(["category"]), $aggSum);
if ($chunkedSmall && $inmemSum) {
    $cRecs = get_records($chunkedSmall, $ffi);
    $iRecs = get_records($inmemSum, $ffi);
    $cMap = []; foreach ($cRecs as $r) { $cMap[$r["category"]] = floatval($r["price"]); }
    $iMap = []; foreach ($iRecs as $r) { $iMap[$r["category"]] = floatval($r["price"]); }
    $allMatch = true;
    foreach ($iMap as $cat => $val) {
        if (isset($cMap[$cat]) && abs($cMap[$cat] - $val) > 1.0) {
            $allMatch = false;
            break;
        }
    }
    if ($allMatch) {
        ok("small chunk_size=1000 still produces correct results (tolerance < 1.0)");
    } else {
        fail("small chunk_size results mismatch");
    }
    $ffi->pardox_free_manager($chunkedSmall);
    $ffi->pardox_free_manager($inmemSum);
} else {
    fail("chunked_groupby with small chunk returned null");
}

// =============================================================================
// PART 3: EXTERNAL SORT
// =============================================================================
section("TEST 16 - external_sort: ascending by price (chunk=20000)");
$sortedExt = $ffi->pardox_external_sort($mgrSales, "price", 1, 20000);
if ($sortedExt) {
    ok("external_sort ascending -> " . number_format(nrows($sortedExt, $ffi)) . " rows");
} else {
    fail("external_sort returned null");
}

section("TEST 17 - external_sort: result is actually sorted ascending");
if ($sortedExt) {
    $prices = get_f64_values($sortedExt, "price", $ffi);
    $isSorted = true;
    $checkCount = min(count($prices) - 1, 9999);
    for ($i = 0; $i < $checkCount; $i++) {
        if ($prices[$i] > $prices[$i+1]) {
            $isSorted = false;
            break;
        }
    }
    if ($isSorted) {
        ok("prices are sorted ascending (checked ".($checkCount+1)." values)");
        echo "    first 5: ".json_encode(array_slice($prices, 0, 5))."\n";
        echo "    last  5: ".json_encode(array_slice($prices, -5))."\n";
    } else {
        fail("prices are NOT sorted ascending");
    }
    $ffi->pardox_free_manager($sortedExt);
}

section("TEST 18 - external_sort: descending by price");
$sortedDesc = $ffi->pardox_external_sort($mgrSales, "price", 0, 25000);
if ($sortedDesc) {
    $prices = get_f64_values($sortedDesc, "price", $ffi);
    $isSorted = true;
    $checkCount = min(count($prices) - 1, 9999);
    for ($i = 0; $i < $checkCount; $i++) {
        if ($prices[$i] < $prices[$i+1]) {
            $isSorted = false;
            break;
        }
    }
    if ($isSorted) {
        ok("prices sorted descending (checked ".($checkCount+1)." values)");
    } else {
        fail("prices NOT sorted descending");
    }
    $ffi->pardox_free_manager($sortedDesc);
} else {
    fail("external_sort descending returned null");
}

section("TEST 19 - external_sort: row count preserved");
$sortedCheck = $ffi->pardox_external_sort($mgrSales, "price", 1, 30000);
if ($sortedCheck) {
    $orig = nrows($mgrSales, $ffi);
    $rest = nrows($sortedCheck, $ffi);
    if ($orig == $rest) {
        ok("row count preserved after sort: " . number_format($orig));
    } else {
        fail("row count mismatch: orig=".number_format($orig).", sorted=".number_format($rest));
    }
    $ffi->pardox_free_manager($sortedCheck);
} else {
    fail("external_sort returned null");
}

section("TEST 20 - external_sort: small chunk (5000 rows)");
$sortedSmall = $ffi->pardox_external_sort($mgrSales, "price", 1, 5000);
if ($sortedSmall) {
    $prices = get_f64_values($sortedSmall, "price", $ffi);
    $isSorted = true;
    $checkCount = min(count($prices) - 1, 9999);
    for ($i = 0; $i < $checkCount; $i++) {
        if ($prices[$i] > $prices[$i+1]) {
            $isSorted = false;
            break;
        }
    }
    if ($isSorted) {
        ok("small chunk_size=5000 produces correct sorted result");
    } else {
        fail("small chunk sort is NOT sorted");
    }
    $ffi->pardox_free_manager($sortedSmall);
} else {
    fail("external_sort small chunk returned null");
}

section("TEST 21 - external_sort: compare with in-memory sort");
$sortedExt2 = $ffi->pardox_external_sort($mgrSales, "price", 1, 20000);
$sortedMem = $ffi->pardox_sort_values($mgrSales, "price", 0);
if ($sortedExt2 && $sortedMem) {
    $extPrices = get_f64_values($sortedExt2, "price", $ffi);
    $memPrices = get_f64_values($sortedMem, "price", $ffi);
    if (count($extPrices) == count($memPrices)) {
        $maxDiff = 0;
        $checkCount = min(5000, count($extPrices));
        for ($i = 0; $i < $checkCount; $i++) {
            $maxDiff = max($maxDiff, abs($extPrices[$i] - $memPrices[$i]));
        }
        if ($maxDiff < 1e-10) {
            ok("external sort matches in-memory sort (max_diff=$maxDiff)");
        } else {
            fail("sort values differ: max_diff=$maxDiff");
        }
    } else {
        fail("length mismatch");
    }
    $ffi->pardox_free_manager($sortedExt2);
    $ffi->pardox_free_manager($sortedMem);
} else {
    fail("one of ext/mem sort returned null");
}

section("TEST 22 - external_sort: wrong column type returns null");
$sortedBad = $ffi->pardox_external_sort($mgrSales, "category", 1, 10000);
if ($sortedBad === null || $sortedBad == 0) {
    ok("external_sort on Utf8 column correctly returns null");
} else {
    fail("external_sort on Utf8 should return null");
    $ffi->pardox_free_manager($sortedBad);
}

// =============================================================================
// PART 4: MEMORY USAGE
// =============================================================================
section("TEST 23 - memory_usage: returns positive value");
$mem = $ffi->pardox_memory_usage();
if ($mem > 0) {
    $mb = $mem / (1024 * 1024);
    ok("memory_usage -> $mem bytes ($mb MB)");
} else {
    fail("memory_usage returned $mem, expected > 0");
}

section("TEST 24 - memory_usage: reasonable range");
$mem = $ffi->pardox_memory_usage();
$mb = $mem / (1024 * 1024);
if ($mb > 1 && $mb < 16000) {
    ok("memory_usage is reasonable: $mb MB");
} else {
    fail("memory_usage seems off: $mb MB");
}

// =============================================================================
// CLEANUP
// =============================================================================
section("CLEANUP");
$ffi->pardox_free_manager($mgrSales);
echo "  Freed all managers.\n";

// =============================================================================
// SUMMARY
// =============================================================================
echo "\n" . str_repeat("=", 60) . "\n";
echo "  Gap 11 - Spill to Disk: RESULTS\n";
echo str_repeat("=", 60) . "\n";
echo "  Passed: $passed\n";
echo "  Failed: $failed\n";
echo "  Total:  " . ($passed + $failed) . "\n";
echo str_repeat("=", 60) . "\n";

if ($failed > 0) {
    echo "\n  SOME TESTS FAILED ($failed)\n";
    exit(1);
} else {
    echo "\n  ALL $passed TESTS PASSED\n";
    exit(0);
}
