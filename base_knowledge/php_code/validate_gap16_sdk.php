#!/usr/bin/env php
<?php
/**
 * validate_gap16_sdk.php - Gap 16: Live Query / Auto-Refresh
 */

require_once __DIR__ . '/pardox/vendor/autoload.php';

use PardoX\DataFrame;

$CSV_SALES = 'sales.csv';
$PRDX_PATH = 'ventas_consolidado.prdx';

if (!file_exists($CSV_SALES)) {
    fwrite(STDERR, "Missing: $CSV_SALES\n");
    exit(1);
}

$results = [];

function check(string $name, bool $cond, string $detail = ''): void {
    global $results;
    $status = $cond ? 'PASS' : 'FAIL';
    $results[] = [$name, $status, $detail];
    $mark = $cond ? '✓' : '✗';
    echo "  [$mark] $name" . ($detail ? "  - $detail" : '') . "\n";
}

echo "\n[TEST] Live Query Functions...\n";

$ffi = \PardoX\Core\FFI::getInstance();

// Check if functions exist
try {
    $hasStart = $ffi->pardox_live_query_start !== null;
} catch (\Throwable $e) { $hasStart = false; }
try {
    $hasVersion = $ffi->pardox_live_query_version !== null;
} catch (\Throwable $e) { $hasVersion = false; }
try {
    $hasTake = $ffi->pardox_live_query_take !== null;
} catch (\Throwable $e) { $hasTake = false; }

check('pardox_live_query_start exists', $hasStart);
check('pardox_live_query_version exists', $hasVersion);
check('pardox_live_query_take exists', $hasTake);

if ($hasStart && file_exists($PRDX_PATH)) {
    $sql = "SELECT category, SUM(price) AS total FROM t GROUP BY category";
    $lq = $ffi->pardox_live_query_start($PRDX_PATH, $sql, 250);
    check('pardox_live_query_start returns non-null', $lq !== null);

    if ($lq) {
        // Wait for version 1
        $version = 0;
        for ($i = 0; $i < 50 && $version < 1; $i++) {
            usleep(100000);
            $version = $ffi->pardox_live_query_version($lq);
        }
        check('Live query version reaches 1', $version >= 1, "version=$version");

        $result = $ffi->pardox_live_query_take($lq);
        if ($result) {
            $rows = $ffi->pardox_get_row_count($result);
            check('Live query result has rows', $rows > 0, "$rows rows");
            $ffi->pardox_free_manager($result);
        }

        $vBefore = $ffi->pardox_live_query_version($lq);
        usleep(300000);
        $vAfter = $ffi->pardox_live_query_version($lq);
        check('Version stable when unchanged', $vBefore == $vAfter);

        $ffi->pardox_live_query_free($lq);
    }
} else {
    check('pardox_live_query_start returns non-null', false, $hasStart ? 'PRDX not found' : 'function missing');
}

echo "\n" . str_repeat('=', 60) . "\n";
echo "Gap 16 - Live Query Results\n";
echo str_repeat('=', 60) . "\n";

$passed = count(array_filter($results, fn($r) => $r[1] === 'PASS'));
$failed = count(array_filter($results, fn($r) => $r[1] === 'FAIL'));
echo "  Results: $passed/$passed+$failed passed\n";
echo str_repeat('=', 60) . "\n";

exit($failed > 0 ? 1 : 0);