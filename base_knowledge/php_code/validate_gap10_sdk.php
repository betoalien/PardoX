<?php
/**
 * validate_gap10_sdk.php - Gap 10: Nested / Complex Data (PHP SDK)
 *
 * Tests pardox_json_extract, pardox_explode, pardox_unnest using:
 *   - CSV data with JSON string columns
 *   - Dot-notation nested key extraction
 *   - Array explosion (row multiplication)
 *   - Object unnesting (column expansion)
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

function load_csv($path, $ffi, $delimiter = 124, $schema = null) {
    // Pipe delimiter (124) with NO quote char (0) to handle JSON columns correctly
    $config = json_encode(["delimiter" => $delimiter, "quote_char" => 0, "has_header" => true, "chunk_size" => 16777216]);
    if ($schema !== null) {
        $schemaJson = json_encode($schema);
    } else {
        $schemaJson = "{}";
    }
    return $ffi->pardox_load_manager_csv($path, $schemaJson, $config);
}

// =============================================================================
echo "\n[INIT] Starting PardoX PHP SDK - Gap 10 validation...";

// =============================================================================
// TEST DATA SETUP
// =============================================================================
section("SETUP: Create test CSV files with JSON columns");

// Use pipe delimiter - JSON strings written with single quotes in PHP
$csvObjects = 'test_nested_objects.csv';
file_put_contents($csvObjects,
    "id|name|metadata\n" .
    '1|Alice|{"city":"New York","age":"30","dept":"Engineering"}' . "\n" .
    '2|Bob|{"city":"Los Angeles","age":"25"}' . "\n" .
    '3|Charlie|{"city":"Chicago","age":"35","dept":"Marketing"}' . "\n" .
    '4|Diana|{"city":"Houston","age":"28","dept":"Sales"}' . "\n" .
    '5|Eve|{"city":"Phoenix","age":"32"}' . "\n"
);

$csvArrays = 'test_nested_arrays.csv';
file_put_contents($csvArrays,
    "id|name|tags\n" .
    '1|Alice|["python","rust","sql"]' . "\n" .
    '2|Bob|["java","go"]' . "\n" .
    '3|Charlie|["python"]' . "\n" .
    '4|Diana|["rust","c++","wasm","gpu"]' . "\n" .
    '5|Eve|[]' . "\n"
);

$csvNested = 'test_deep_nested.csv';
file_put_contents($csvNested,
    "id|name|profile\n" .
    '1|Alice|{"address":{"city":"NYC","zip":"10001"},"score":95}' . "\n" .
    '2|Bob|{"address":{"city":"LA","zip":"90001"},"score":88}' . "\n" .
    '3|Charlie|{"address":{"city":"CHI","zip":"60601"},"score":72}' . "\n"
);

$csvMixed = 'test_mixed_explode.csv';
file_put_contents($csvMixed,
    "id|name|skills|salary\n" .
    '1|Alice|["python","rust"]|95000' . "\n" .
    '2|Bob|["java"]|85000' . "\n" .
    '3|Charlie|["go","python","sql"]|90000' . "\n"
);

// Load test data
section("SETUP: Load test CSVs");

// Use pipe delimiter (124) like Python does
$mgrObjects = load_csv($csvObjects, $ffi, 124, null);
$mgrArrays  = load_csv($csvArrays, $ffi, 124, null);
$mgrNested  = load_csv($csvNested, $ffi, 124, null);
$mgrMixed   = load_csv($csvMixed, $ffi, 124, null);

echo "  mgr_objects: " . nrows($mgrObjects, $ffi) . " rows\n";
echo "  mgr_arrays:  " . nrows($mgrArrays, $ffi) . " rows\n";
echo "  mgr_nested:  " . nrows($mgrNested, $ffi) . " rows\n";
echo "  mgr_mixed:   " . nrows($mgrMixed, $ffi) . " rows\n";

// =============================================================================
// PART 1: json_extract
// =============================================================================
section("TEST 1 - json_extract: extract 'city' from metadata");
$r = $ffi->pardox_json_extract($mgrObjects, "metadata", "city");
if ($r) {
    $rows = nrows($r, $ffi);
    $records = get_records($r, $ffi);
    $cols = col_names($r, $ffi);
    $expectedCities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"];
    $actualCities = array_map(fn($rec) => $rec[$cols[0]] ?? "", $records);
    if ($rows == 5 && $actualCities == $expectedCities) {
        ok("json_extract('city') -> $rows rows, cities match: ".json_encode($actualCities));
    } else {
        fail("json_extract('city')", "rows=$rows, cities=".json_encode($actualCities).", expected=".json_encode($expectedCities));
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("json_extract('city') returned null");
}

section("TEST 2 - json_extract: extract 'age' from metadata");
$r = $ffi->pardox_json_extract($mgrObjects, "metadata", "age");
if ($r) {
    $records = get_records($r, $ffi);
    $cols = col_names($r, $ffi);
    $expectedAges = ["30", "25", "35", "28", "32"];
    $actualAges = array_map(fn($rec) => $rec[$cols[0]] ?? "", $records);
    if ($actualAges == $expectedAges) {
        ok("json_extract('age') -> ages match: ".json_encode($actualAges));
    } else {
        fail("json_extract('age')", json_encode($actualAges)." != ".json_encode($expectedAges));
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("json_extract('age') returned null");
}

section("TEST 3 - json_extract: extract 'dept' (missing in some rows)");
$r = $ffi->pardox_json_extract($mgrObjects, "metadata", "dept");
if ($r) {
    $records = get_records($r, $ffi);
    $cols = col_names($r, $ffi);
    $expected = ["Engineering", "", "Marketing", "Sales", ""];
    $actual = array_map(fn($rec) => $rec[$cols[0]] ?? "", $records);
    if ($actual == $expected) {
        ok("json_extract('dept') handles missing keys: ".json_encode($actual));
    } else {
        fail("json_extract('dept')", json_encode($actual)." != ".json_encode($expected));
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("json_extract('dept') returned null");
}

section("TEST 4 - json_extract: dot-notation 'address.city'");
$r = $ffi->pardox_json_extract($mgrNested, "profile", "address.city");
if ($r) {
    $records = get_records($r, $ffi);
    $cols = col_names($r, $ffi);
    $expected = ["NYC", "LA", "CHI"];
    $actual = array_map(fn($rec) => $rec[$cols[0]] ?? "", $records);
    if ($actual == $expected) {
        ok("json_extract('address.city') dot-notation: ".json_encode($actual));
    } else {
        fail("json_extract('address.city')", json_encode($actual)." != ".json_encode($expected));
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("json_extract('address.city') returned null");
}

section("TEST 5 - json_extract: dot-notation 'address.zip'");
$r = $ffi->pardox_json_extract($mgrNested, "profile", "address.zip");
if ($r) {
    $records = get_records($r, $ffi);
    $cols = col_names($r, $ffi);
    $expected = ["10001", "90001", "60601"];
    $actual = array_map(fn($rec) => $rec[$cols[0]] ?? "", $records);
    if ($actual == $expected) {
        ok("json_extract('address.zip') dot-notation: ".json_encode($actual));
    } else {
        fail("json_extract('address.zip')", json_encode($actual)." != ".json_encode($expected));
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("json_extract('address.zip') returned null");
}

section("TEST 6 - json_extract: numeric value 'score'");
$r = $ffi->pardox_json_extract($mgrNested, "profile", "score");
if ($r) {
    $records = get_records($r, $ffi);
    $cols = col_names($r, $ffi);
    $expected = ["95", "88", "72"];
    $actual = array_map(fn($rec) => $rec[$cols[0]] ?? "", $records);
    if ($actual == $expected) {
        ok("json_extract('score') numeric value as string: ".json_encode($actual));
    } else {
        fail("json_extract('score')", json_encode($actual)." != ".json_encode($expected));
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("json_extract('score') returned null");
}

section("TEST 7 - json_extract: nonexistent key returns empty strings");
$r = $ffi->pardox_json_extract($mgrObjects, "metadata", "nonexistent");
if ($r) {
    $records = get_records($r, $ffi);
    $cols = col_names($r, $ffi);
    $actual = array_map(fn($rec) => $rec[$cols[0]] ?? "X", $records);
    $allEmpty = true;
    foreach ($actual as $v) {
        if ($v !== "") $allEmpty = false;
    }
    if ($allEmpty) {
        ok("json_extract('nonexistent') -> all empty strings");
    } else {
        fail("json_extract('nonexistent')", json_encode($actual).", expected all empty");
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("json_extract('nonexistent') returned null");
}

// =============================================================================
// PART 2: explode - JSON array expansion
// =============================================================================
section("TEST 8 - explode: expand 'tags' array");
$r = $ffi->pardox_explode($mgrArrays, "tags");
if ($r) {
    $rows = nrows($r, $ffi);
    // Expected: 3+2+1+4+1(empty) = 11 rows  (Eve's empty array -> 1 row with "")
    if ($rows == 11) {
        ok("explode('tags') -> $rows rows (3+2+1+4+1=11)");
    } else {
        fail("explode('tags')", "$rows rows, expected 11");
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("explode('tags') returned null");
}

section("TEST 9 - explode: verify row values after explosion");
$r = $ffi->pardox_explode($mgrArrays, "tags");
if ($r) {
    $records = get_records($r, $ffi);
    $aliceTags = array_filter(array_map(fn($rec) => $rec["tags"] ?? null,
        array_filter($records, fn($rec) => ($rec["name"] ?? "") == "Alice")));
    $bobTags = array_filter(array_map(fn($rec) => $rec["tags"] ?? null,
        array_filter($records, fn($rec) => ($rec["name"] ?? "") == "Bob")));
    sort($aliceTags); sort($bobTags);
    $expectedAlice = ["python", "rust", "sql"];
    $expectedBob = ["go", "java"];
    sort($expectedAlice); sort($expectedBob);
    if ($aliceTags == $expectedAlice && $bobTags == $expectedBob) {
        ok("explode values correct: Alice=".json_encode(array_values($aliceTags)).", Bob=".json_encode(array_values($bobTags)));
    } else {
        fail("explode values", "Alice=".json_encode(array_values($aliceTags))." (exp ".json_encode($expectedAlice).
             "), Bob=".json_encode(array_values($bobTags))." (exp ".json_encode($expectedBob).")");
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("explode('tags') returned null");
}

section("TEST 10 - explode: empty array produces 1 row with empty string");
$r = $ffi->pardox_explode($mgrArrays, "tags");
if ($r) {
    $records = get_records($r, $ffi);
    $eveTags = array_values(array_map(fn($rec) => $rec["tags"] ?? null,
        array_filter($records, fn($rec) => ($rec["name"] ?? "") == "Eve")));
    if ($eveTags == [""]) {
        ok("explode empty array -> Eve has 1 row with empty tag: ".json_encode($eveTags));
    } else {
        fail("explode empty array", "Eve tags: ".json_encode($eveTags).", expected ['']");
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("explode returned null");
}

section("TEST 11 - explode: other columns repeated correctly");
$r = $ffi->pardox_explode($mgrArrays, "tags");
if ($r) {
    $records = get_records($r, $ffi);
    // Diana has 4 tags, so id=4 should appear 4 times
    $dianaIds = array_map(fn($rec) => $rec["id"] ?? null,
        array_filter($records, fn($rec) => ($rec["name"] ?? "") == "Diana"));
    if (count($dianaIds) == 4) {
        ok("explode: Diana's id repeated 4 times: ".json_encode($dianaIds));
    } else {
        fail("explode: Diana should have 4 rows", "got ".count($dianaIds).": ".json_encode($dianaIds));
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("explode returned null");
}

section("TEST 12 - explode: mixed data (skills + salary columns)");
$r = $ffi->pardox_explode($mgrMixed, "skills");
if ($r) {
    $rows = nrows($r, $ffi);
    $records = get_records($r, $ffi);
    $cols = col_names($r, $ffi);
    // 2 + 1 + 3 = 6 rows
    if ($rows == 6) {
        ok("explode mixed data -> $rows rows (2+1+3=6)");
    } else {
        fail("explode mixed data", "$rows rows, expected 6");
    }
    // Check salary is preserved
    if (in_array("salary", $cols)) {
        ok("explode preserves 'salary' column in schema: ".json_encode($cols));
    } else {
        fail("explode lost 'salary' column", json_encode($cols));
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("explode('skills') returned null");
}

// =============================================================================
// PART 3: unnest - JSON object flattening
// =============================================================================
section("TEST 13 - unnest: flatten 'metadata' object into columns");
$r = $ffi->pardox_unnest($mgrObjects, "metadata");
if ($r) {
    $rows = nrows($r, $ffi);
    $cols = col_names($r, $ffi);
    if ($rows == 5) {
        ok("unnest('metadata') -> $rows rows preserved");
    } else {
        fail("unnest('metadata')", "$rows rows, expected 5");
    }
    // Should have columns like: id, name, metadata_city, metadata_age, metadata_dept
    $found = array_filter($cols, fn($c) => str_starts_with($c, "metadata_"));
    $expectedCols = ["metadata_city", "metadata_age", "metadata_dept"];
    $allFound = true;
    foreach ($expectedCols as $c) {
        if (!in_array($c, $found)) $allFound = false;
    }
    if ($allFound) {
        ok("unnest created columns: ".json_encode(array_values($found)));
    } else {
        fail("unnest columns", json_encode($found).", expected at least ".json_encode($expectedCols));
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("unnest('metadata') returned null");
}

section("TEST 14 - unnest: verify extracted values");
$r = $ffi->pardox_unnest($mgrObjects, "metadata");
if ($r) {
    $records = get_records($r, $ffi);
    $cities = array_map(fn($rec) => $rec["metadata_city"] ?? "", $records);
    $expectedCities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"];
    if ($cities == $expectedCities) {
        ok("unnest cities: ".json_encode($cities));
    } else {
        fail("unnest cities", json_encode($cities)." != ".json_encode($expectedCities));
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("unnest returned null");
}

section("TEST 15 - unnest: missing keys produce empty strings");
$r = $ffi->pardox_unnest($mgrObjects, "metadata");
if ($r) {
    $records = get_records($r, $ffi);
    $depts = array_map(fn($rec) => $rec["metadata_dept"] ?? "MISSING", $records);
    $expectedDepts = ["Engineering", "", "Marketing", "Sales", ""];
    if ($depts == $expectedDepts) {
        ok("unnest missing dept values -> empty strings: ".json_encode($depts));
    } else {
        fail("unnest depts", json_encode($depts)." != ".json_encode($expectedDepts));
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("unnest returned null");
}

section("TEST 16 - unnest: original JSON column removed");
$r = $ffi->pardox_unnest($mgrObjects, "metadata");
if ($r) {
    $cols = col_names($r, $ffi);
    if (!in_array("metadata", $cols)) {
        ok("unnest removed original 'metadata' column: ".json_encode($cols));
    } else {
        fail("unnest should remove 'metadata' but it's still in", json_encode($cols));
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("unnest returned null");
}

section("TEST 17 - unnest: original columns preserved");
$r = $ffi->pardox_unnest($mgrObjects, "metadata");
if ($r) {
    $cols = col_names($r, $ffi);
    if (in_array("id", $cols) && in_array("name", $cols)) {
        ok("unnest preserves 'id' and 'name' columns: ".json_encode($cols));
    } else {
        fail("unnest lost original columns", json_encode($cols));
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("unnest returned null");
}

// =============================================================================
// PART 4: Edge cases
// =============================================================================
section("TEST 18 - json_extract: wrong column type returns null");
// Try to extract from 'id' which is Int64, not Utf8
$r = $ffi->pardox_json_extract($mgrObjects, "id", "key");
if ($r === null || $r == 0) {
    ok("json_extract on non-string column correctly returned null");
} else {
    fail("json_extract on non-string column should return null");
    $ffi->pardox_free_manager($r);
}

section("TEST 19 - explode: wrong column type returns null");
$r = $ffi->pardox_explode($mgrObjects, "id");
if ($r === null || $r == 0) {
    ok("explode on non-string column correctly returned null");
} else {
    fail("explode on non-string column should return null");
    $ffi->pardox_free_manager($r);
}

section("TEST 20 - json_extract: nonexistent column returns null");
$r = $ffi->pardox_json_extract($mgrObjects, "nonexistent_col", "key");
if ($r === null || $r == 0) {
    ok("json_extract on missing column correctly returned null");
} else {
    fail("json_extract on missing column should return null");
    $ffi->pardox_free_manager($r);
}

section("TEST 21 - json_extract: output column naming");
$r = $ffi->pardox_json_extract($mgrObjects, "metadata", "city");
if ($r) {
    $cols = col_names($r, $ffi);
    if ($cols && str_contains($cols[0], "metadata_city")) {
        ok("json_extract output column named: ".$cols[0]);
    } else {
        fail("json_extract output column", json_encode($cols).", expected 'metadata_city'");
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("json_extract returned null");
}

section("TEST 22 - explode: schema preserved");
$r = $ffi->pardox_explode($mgrArrays, "tags");
if ($r) {
    $cols = col_names($r, $ffi);
    if (in_array("id", $cols) && in_array("name", $cols) && in_array("tags", $cols)) {
        ok("explode schema: ".json_encode($cols));
    } else {
        fail("explode schema missing expected columns", json_encode($cols));
    }
    $ffi->pardox_free_manager($r);
} else {
    fail("explode returned null");
}

// =============================================================================
// CLEANUP
// =============================================================================
section("CLEANUP");
$ffi->pardox_free_manager($mgrObjects);
$ffi->pardox_free_manager($mgrArrays);
$ffi->pardox_free_manager($mgrNested);
$ffi->pardox_free_manager($mgrMixed);
echo "  Freed all managers.\n";

foreach (["test_nested_objects.csv", "test_nested_arrays.csv",
          "test_deep_nested.csv", "test_mixed_explode.csv"] as $f) {
    $p = $here . '/' . $f;
    if (file_exists($p)) @unlink($p);
}
echo "  Removed test CSV files.\n";

// =============================================================================
// SUMMARY
// =============================================================================
echo "\n" . str_repeat("=", 60) . "\n";
echo "  Gap 10 - Nested / Complex Data: RESULTS\n";
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
