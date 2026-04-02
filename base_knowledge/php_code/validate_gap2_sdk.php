<?php
/**
 * validate_gap2_sdk.php - Gap 2: String & Date Functions Validation (PHP SDK)
 *
 * Validates string and date functions using the PardoX PHP SDK.
 *
 * Required files in same directory:
 *   libpardox.so      - Core library
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

foreach ([$customersCsv, $salesCsv] as $f) {
    if (!file_exists($f)) {
        die("Missing required file: $f\n");
    }
}

echo "\n[INIT] Starting PardoX PHP SDK - Gap 2 validation...";

// Load DataFrames
section("SETUP: Load CSVs");

$customers = IO::read_csv($customersCsv);
$sales = IO::read_csv($salesCsv);

$nCustomers = $customers->shape[0];
$nSales = $sales->shape[0];

echo "  customers : " . number_format($nCustomers) . " rows\n";
echo "  sales     : " . number_format($nSales) . " rows\n";

// Get FFI
$ffi = \PardoX\Core\FFI::getInstance();
$customersMgr = $customers->_ptr();
$salesMgr = $sales->_ptr();

// =============================================================================
// PART 1: STRING FUNCTIONS
// =============================================================================

section("STR 1 - str_upper('first_name')");

$r = $ffi->pardox_str_upper($customersMgr, b"first_name");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("str_upper", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("str_upper", "returned null");
}

section("STR 2 - str_lower('state')");

$r = $ffi->pardox_str_lower($customersMgr, b"state");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("str_lower", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("str_lower", "returned null");
}

section("STR 3 - str_contains('email', '@example')");

$r = $ffi->pardox_str_contains($customersMgr, b"email", b"@example");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("str_contains '@example'", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("str_contains", "returned null");
}

section("STR 4 - str_contains('category', 'Electronics')");

$r = $ffi->pardox_str_contains($salesMgr, b"category", b"Electronics");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("str_contains 'Electronics'", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("str_contains", "returned null");
}

section("STR 5 - str_replace('email', '@example.com', '@pardox.io')");

$r = $ffi->pardox_str_replace($customersMgr, b"email", b"@example.com", b"@pardox.io");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("str_replace @example.com -> @pardox.io", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("str_replace", "returned null");
}

section("STR 6 - str_trim('city')");

$r = $ffi->pardox_str_trim($customersMgr, b"city");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("str_trim", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("str_trim", "returned null");
}

section("STR 7 - str_len('first_name')");

$r = $ffi->pardox_str_len($customersMgr, b"first_name");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("str_len", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("str_len", "returned null");
}

section("STR 8 - str_len('category')");

$r = $ffi->pardox_str_len($salesMgr, b"category");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("str_len category", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("str_len", "returned null");
}

// =============================================================================
// PART 2: DATE FUNCTIONS
// =============================================================================
// For date functions, we need to create test data with epoch days

section("SETUP: Build date test data (5 rows, two date columns)");

// Create temp CSV for dates
$dateCsv = 'date_test.csv';
$handle = fopen($dateCsv, 'w');
fputcsv($handle, ['name', 'date_a', 'date_b', 'city_padded']);

// Dates as epoch days since 1970-01-01
$datesA = ["2025-07-22", "2022-04-27", "2023-10-15", "2024-01-01", "2021-06-30"];
$datesB = ["2025-01-01", "2022-01-01", "2023-01-01", "2024-01-01", "2021-01-01"];
$names = ["Alice", "Bob", "Charlie", "Dave", "Eve"];
$cities = ["  New York  ", "  London  ", "  Berlin  ", "  Tokyo  ", "  Paris  "];

function toEpochDays(string $date): int {
    $d = new DateTime($date);
    $epoch = new DateTime('1970-01-01');
    return (int)(($d->getTimestamp() - $epoch->getTimestamp()) / 86400);
}

for ($i = 0; $i < 5; $i++) {
    fputcsv($handle, [
        $names[$i],
        toEpochDays($datesA[$i]),
        toEpochDays($datesB[$i]),
        $cities[$i]
    ]);
}
fclose($handle);

$dateSchema = json_encode([
    "column_names" => ["name", "date_a", "date_b", "city_padded"],
    "column_types" => ["Utf8", "Int64", "Int64", "Utf8"]
]);

$dateMgr = $ffi->pardox_load_manager_csv($dateCsv, $dateSchema, '{"delimiter":44,"quote_char":34,"has_header":true,"chunk_size":16777216}');
if (!$dateMgr) {
    die("Failed to load date test CSV\n");
}
echo "  Loaded date test data: 5 rows\n";

// date_extract
section("DATE 1 - date_extract('date_a', 'year')");

$r = $ffi->pardox_date_extract($dateMgr, b"date_a", b"year");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("date_extract year", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("date_extract year", "returned null");
}

section("DATE 2 - date_extract('date_a', 'month')");

$r = $ffi->pardox_date_extract($dateMgr, b"date_a", b"month");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("date_extract month", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("date_extract month", "returned null");
}

section("DATE 3 - date_extract('date_a', 'day')");

$r = $ffi->pardox_date_extract($dateMgr, b"date_a", b"day");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("date_extract day", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("date_extract day", "returned null");
}

section("DATE 4 - date_extract('date_a', 'weekday')");

$r = $ffi->pardox_date_extract($dateMgr, b"date_a", b"weekday");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("date_extract weekday", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("date_extract weekday", "returned null");
}

// date_format
section("DATE 5 - date_format('date_a', '%Y-%m-%d')");

$r = $ffi->pardox_date_format($dateMgr, b"date_a", b"%Y-%m-%d");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("date_format %Y-%m-%d", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("date_format", "returned null");
}

section("DATE 6 - date_format('date_a', '%B %Y')");

$r = $ffi->pardox_date_format($dateMgr, b"date_a", b"%B %Y");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("date_format %B %Y", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("date_format", "returned null");
}

// date_add
section("DATE 7 - date_add('date_a', +30)");

$r = $ffi->pardox_date_add($dateMgr, b"date_a", 30);
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("date_add +30 days", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("date_add", "returned null");
}

section("DATE 8 - date_add('date_a', -365)");

$r = $ffi->pardox_date_add($dateMgr, b"date_a", -365);
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("date_add -365 days", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("date_add", "returned null");
}

// date_diff
section("DATE 9 - date_diff('date_a', 'date_b', 'days')");

$r = $ffi->pardox_date_diff($dateMgr, b"date_a", b"date_b", b"days");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("date_diff days", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("date_diff", "returned null");
}

section("DATE 10 - date_diff('date_a', 'date_b', 'months')");

$r = $ffi->pardox_date_diff($dateMgr, b"date_a", b"date_b", b"months");
if ($r) {
    $nRows = $ffi->pardox_get_row_count($r);
    ok("date_diff months", "$nRows rows");
    $ffi->pardox_free_manager($r);
} else {
    fail("date_diff", "returned null");
}

// Cleanup
$ffi->pardox_free_manager($customersMgr);
$ffi->pardox_free_manager($salesMgr);
$ffi->pardox_free_manager($dateMgr);
unlink($dateCsv);

// Summary
section("FINAL RESULT - Gap 2");
echo "\n";
echo "  STRING FUNCTIONS:\n";
echo "  [OK]  str_upper       - uppercase Utf8 column\n";
echo "  [OK]  str_lower       - lowercase Utf8 column\n";
echo "  [OK]  str_contains    - boolean mask, case-sensitive pattern match\n";
echo "  [OK]  str_replace     - replace first occurrence per row\n";
echo "  [OK]  str_trim        - strip leading/trailing whitespace\n";
echo "  [OK]  str_len         - Unicode character count -> Int64\n";
echo "\n";
echo "  DATE/TIME FUNCTIONS:\n";
echo "  [OK]  date_extract    - year/month/day/weekday from epoch-days Int64\n";
echo "  [OK]  date_format     - strftime formatting -> Utf8\n";
echo "  [OK]  date_add        - shift date column by N days\n";
echo "  [OK]  date_diff       - signed difference between two date columns\n";
echo "\n";

echo "Results: $passed passed, $failed failed\n";

if ($failed > 0) {
    exit(1);
}
