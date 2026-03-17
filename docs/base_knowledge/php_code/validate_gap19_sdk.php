#!/usr/bin/env php
<?php
/**
 * validate_gap19_sdk.php - Gap 19: Data Contracts / Schema Validation
 */

require_once __DIR__ . '/pardox/vendor/autoload.php';

$CSV_SALES = 'sales.csv';
$CSV_CUST  = 'customers.csv';

foreach ([$CSV_SALES, $CSV_CUST] as $f) {
    if (!file_exists($f)) {
        fwrite(STDERR, "Missing: $f\n");
        exit(1);
    }
}

$results = [];

function check(string $name, bool $cond, string $detail = ''): void {
    global $results;
    $status = $cond ? 'PASS' : 'FAIL';
    $results[] = [$name, $status, $detail];
    $mark = $cond ? '✓' : '✗';
    echo "  [$mark] $name" . ($detail ? "  - $detail" : '') . "\n";
}

echo "\n[TEST] Data Contracts Functions...\n";

$ffi = \PardoX\Core\FFI::getInstance();

// Check if functions exist
try {
    $hasValidate = $ffi->pardox_validate_contract !== null;
} catch (\Throwable $e) { $hasValidate = false; }
try {
    $hasViolationCount = $ffi->pardox_contract_violation_count !== null;
} catch (\Throwable $e) { $hasViolationCount = false; }
try {
    $hasQuarantineLogs = $ffi->pardox_get_quarantine_logs !== null;
} catch (\Throwable $e) { $hasQuarantineLogs = false; }
try {
    $hasClearQuarantine = $ffi->pardox_clear_quarantine !== null;
} catch (\Throwable $e) { $hasClearQuarantine = false; }

check('pardox_validate_contract exists', $hasValidate);
check('pardox_contract_violation_count exists', $hasViolationCount);
check('pardox_get_quarantine_logs exists', $hasQuarantineLogs);
check('pardox_clear_quarantine exists', $hasClearQuarantine);

if ($hasValidate && $hasViolationCount && file_exists($CSV_SALES)) {
    // Test CSV with known violations
    $knownCsv = "id,name,score,category\n1,Alice,95.0,A\n2,Bob,-5.0,A\n3,Carol,105.0,B\n4,Dave,75.0,X\n5,Eve,80.0,A\n6,,60.0,B\n";

    // Create temp file
    $tmpFile = 'test_contracts.csv';
    file_put_contents($tmpFile, $knownCsv);

    $cfg = '{"delimiter":44,"has_header":true}';
    $mgr = $ffi->pardox_load_manager_csv($tmpFile, '{}', $cfg);
    check('Load test CSV', $mgr !== null);

    if ($mgr) {
        $contract = json_encode([
            "source" => "test_dataset",
            "columns" => [
                "score" => ["nullable" => false, "min" => 0.0, "max" => 100.0],
                "name" => ["nullable" => false],
                "category" => ["allowed_values" => ["A", "B"]]
            ]
        ]);

        $clean = $ffi->pardox_validate_contract($mgr, $contract);
        check('Validate contract returns manager', $clean !== null);

        if ($clean) {
            $cleanRows = $ffi->pardox_get_row_count($clean);
            check('2 rows pass constraints (Alice, Eve)', $cleanRows == 2, "$cleanRows rows");

            $violations = $ffi->pardox_contract_violation_count();
            check('4 violations logged', $violations == 4, "$violations violations");

            $logsJson = $ffi->pardox_get_quarantine_logs();
            if ($logsJson) {
                $logs = json_decode(\FFI::string($logsJson), true);
                $hasStructure = count($logs) > 0 &&
                    array_filter($logs, fn($e) => isset($e['source_id']) && isset($e['row_index']) && isset($e['reason']));
                check('Quarantine logs have correct structure', $hasStructure, count($logs) . " entries");
            }

            $ffi->pardox_free_manager($clean);
        }

        $ffi->pardox_free_manager($mgr);
    }

    $ffi->pardox_clear_quarantine();
    @unlink($tmpFile);

    // Test with sales.csv
    $mgrSales = $ffi->pardox_load_manager_csv($CSV_SALES, '{}', $cfg);
    if ($mgrSales) {
        $contractSales = json_encode([
            "source" => "sales_contract",
            "columns" => [
                "price" => ["min" => 0.0, "max" => 10000.0]
            ]
        ]);

        $cleanSales = $ffi->pardox_validate_contract($mgrSales, $contractSales);
        if ($cleanSales) {
            $origRows = $ffi->pardox_get_row_count($mgrSales);
            $cleanRowsSales = $ffi->pardox_get_row_count($cleanSales);
            $violationsSales = $ffi->pardox_contract_violation_count();

            check('Large dataset: pass + violations = original',
                $cleanRowsSales + $violationsSales == $origRows,
                "pass=$cleanRowsSales violations=$violationsSales orig=$origRows");

            $ffi->pardox_free_manager($cleanSales);
        }

        $ffi->pardox_free_manager($mgrSales);
    }

    $ffi->pardox_clear_quarantine();
}

echo "\n" . str_repeat('=', 60) . "\n";
echo "Gap 19 - Data Contracts Results\n";
echo str_repeat('=', 60) . "\n";

$passed = count(array_filter($results, fn($r) => $r[1] === 'PASS'));
$failed = count(array_filter($results, fn($r) => $r[1] === 'FAIL'));
echo "  Results: $passed/$passed+$failed passed\n";
echo str_repeat('=', 60) . "\n";

if ($failed > 0) {
    echo "\n  FAILED TESTS:\n";
    foreach (array_filter($results, fn($r) => $r[1] === 'FAIL') as $r) {
        echo "    - {$r[0]}: {$r[2]}\n";
    }
    exit(1);
} else {
    echo "\n  ALL TESTS PASSED ✓\n";
    exit(0);
}