#!/usr/bin/env php
<?php
/**
 * validate_gap23_sdk.php - Gap 23: SQL Server Fix
 */

require_once __DIR__ . '/pardox/vendor/autoload.php';

$results = [];

function check(string $name, bool $cond, string $detail = ''): void {
    global $results;
    $status = $cond ? 'PASS' : 'FAIL';
    $results[] = [$name, $status, $detail];
    $mark = $cond ? '✓' : '✗';
    echo "  [$mark] $name" . ($detail ? "  - $detail" : '') . "\n";
}

echo "\n[TEST] SQL Server Functions...\n";

$ffi = \PardoX\Core\FFI::getInstance();

// Check functions (already in FFI from earlier)
try {
    $hasSqlServerConfigOk = $ffi->pardox_sqlserver_config_ok !== null;
} catch (\Throwable $e) { $hasSqlServerConfigOk = false; }
try {
    $hasReadSqlServer = $ffi->pardox_read_sqlserver !== null;
} catch (\Throwable $e) { $hasReadSqlServer = false; }
try {
    $hasWriteSqlServer = $ffi->pardox_write_sqlserver !== null;
} catch (\Throwable $e) { $hasWriteSqlServer = false; }
try {
    $hasExecuteSqlServer = $ffi->pardox_execute_sqlserver !== null;
} catch (\Throwable $e) { $hasExecuteSqlServer = false; }

check('pardox_sqlserver_config_ok exists', $hasSqlServerConfigOk);
check('pardox_read_sqlserver exists', $hasReadSqlServer);
check('pardox_write_sqlserver exists', $hasWriteSqlServer);
check('pardox_execute_sqlserver exists', $hasExecuteSqlServer);

echo "\n" . str_repeat('=', 60) . "\n";
echo "Gap 23 - SQL Server Fix Results\n";
echo str_repeat('=', 60) . "\n";

$passed = count(array_filter($results, fn($r) => $r[1] === 'PASS'));
$failed = count(array_filter($results, fn($r) => $r[1] === 'FAIL'));
echo "  Results: $passed/$passed+$failed passed\n";
echo str_repeat('=', 60) . "\n";

exit($failed > 0 ? 1 : 0);