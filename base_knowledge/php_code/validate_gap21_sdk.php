#!/usr/bin/env php
<?php
/**
 * validate_gap21_sdk.php - Gap 21: Arrow Flight
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

echo "\n[TEST] Arrow Flight Functions...\n";

$ffi = \PardoX\Core\FFI::getInstance();

// Check functions
try {
    $hasFlightStart = $ffi->pardox_flight_start !== null;
} catch (\Throwable $e) { $hasFlightStart = false; }
try {
    $hasFlightRead = $ffi->pardox_flight_read !== null;
} catch (\Throwable $e) { $hasFlightRead = false; }
try {
    $hasFlightRegister = $ffi->pardox_flight_register !== null;
} catch (\Throwable $e) { $hasFlightRegister = false; }
try {
    $hasFlightStop = $ffi->pardox_flight_stop !== null;
} catch (\Throwable $e) { $hasFlightStop = false; }

check('pardox_flight_start exists', $hasFlightStart);
check('pardox_flight_read exists', $hasFlightRead);
check('pardox_flight_register exists', $hasFlightRegister);
check('pardox_flight_stop exists', $hasFlightStop);

echo "\n" . str_repeat('=', 60) . "\n";
echo "Gap 21 - Arrow Flight Results\n";
echo str_repeat('=', 60) . "\n";

$passed = count(array_filter($results, fn($r) => $r[1] === 'PASS'));
$failed = count(array_filter($results, fn($r) => $r[1] === 'FAIL'));
echo "  Results: $passed/$passed+$failed passed\n";
echo str_repeat('=', 60) . "\n";

exit($failed > 0 ? 1 : 0);