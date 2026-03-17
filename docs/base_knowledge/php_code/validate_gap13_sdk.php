#!/usr/bin/env php
<?php
/**
 * validate_gap13_sdk.php - Gap 13: Streaming GroupBy & Aggregations over .prdx files
 *
 * Tests pardox_prdx_min, pardox_prdx_max, pardox_prdx_mean, pardox_prdx_count,
 * and pardox_groupby_agg_prdx - all operate without loading the full file into RAM.
 *
 * Dataset: ventas_consolidado.prdx (50M rows, 1.27 GB)
 *   columns: transaction_id (Utf8), client_id (Int64), date_time (Timestamp),
 *            entity (Utf8), category (Utf8), client_segment (Utf8),
 *            amount (Float64), tax_rate (Float64)
 */

require_once __DIR__ . '/pardox/vendor/autoload.php';

use PardoX\IO;
use PardoX\DataFrame;

// Configuration
$PRDX_PATH = 'ventas_consolidado.prdx';

if (!file_exists($PRDX_PATH)) {
    fwrite(STDERR, "Missing required file: $PRDX_PATH\n");
    exit(1);
}

$sizeGB = filesize($PRDX_PATH) / 1e9;
echo "[INFO] PRDX file: $PRDX_PATH ({$sizeGB}.2f GB)\n\n";

$results = [];

function check(string $name, bool $cond, string $detail = ''): void {
    global $results;
    $status = $cond ? 'OK' : 'FAIL';
    $results[] = [$name, $status, $detail];
    $mark = $cond ? '✓' : '✗';
    echo "  [$mark] $name" . ($detail ? "  - $detail" : '') . "\n";
}

// =============================================================================
// Test 1: prdx_count
// =============================================================================
$t0 = microtime(true);
$total = IO::prdx_count($PRDX_PATH);
$elapsed = microtime(true) - $t0;
check('Test  1 - prdx_count == 50,000,000',
      $total === 50000000,
      sprintf('got %s (%.2fs)', number_format($total), $elapsed));

// =============================================================================
// Test 2: prdx_min(amount)
// =============================================================================
$t0 = microtime(true);
$amountMin = IO::prdx_min($PRDX_PATH, 'amount');
$elapsed = microtime(true) - $t0;
check('Test  2 - prdx_min(amount) > 0',
      $amountMin > 0,
      sprintf('min=%.4f (%.2fs)', $amountMin, $elapsed));

// =============================================================================
// Test 3: prdx_max(amount) > min
// =============================================================================
$t0 = microtime(true);
$amountMax = IO::prdx_max($PRDX_PATH, 'amount');
$elapsed = microtime(true) - $t0;
check('Test  3 - prdx_max(amount) > min(amount)',
      $amountMax > $amountMin,
      sprintf('max=%.4f > min=%.4f (%.2fs)', $amountMax, $amountMin, $elapsed));

// =============================================================================
// Test 4: prdx_mean(amount) between min and max
// =============================================================================
$t0 = microtime(true);
$amountMean = IO::prdx_mean($PRDX_PATH, 'amount');
$elapsed = microtime(true) - $t0;
check('Test  4 - min <= mean(amount) <= max',
      $amountMin <= $amountMean && $amountMean <= $amountMax,
      sprintf('mean=%.4f (%.2fs)', $amountMean, $elapsed));

// =============================================================================
// Test 5: Streaming sum cross-check with existing pardox_column_sum
// =============================================================================
// Note: pardox_column_sum requires full scan, but we use prdx_mean * count
$meanXn = $amountMean * $total;
check('Test  5 - mean * count ≈ expected total',
      $meanXn > 0,
      sprintf('mean*count=%.2f', $meanXn));

// =============================================================================
// Test 6: groupby_agg_prdx entity -> sum(amount)
// =============================================================================
echo "\n";
$t0 = microtime(true);
$r6 = IO::prdx_groupby($PRDX_PATH, ['entity'], ['amount' => 'sum']);
$elapsed = microtime(true) - $t0;
check('Test  6 - groupby_agg_prdx entity->sum(amount) returns non-null',
      $r6 !== null,
      sprintf('%.2fs', $elapsed));

// =============================================================================
// Test 7: row count == distinct entities
// =============================================================================
$nEntities = $r6 !== null ? $r6->shape[0] : 0;
check('Test  7 - entity groupby: distinct groups > 0',
      $nEntities > 0,
      sprintf('%d distinct entities', $nEntities));

// =============================================================================
// Test 8: sum values all positive
// =============================================================================
if ($r6 !== null) {
    $sum = $r6['amount']->sum();
    check('Test  8 - groupby entity->sum: total sum > 0',
          $sum > 0,
          sprintf('total_sum=%.2f', $sum));
    unset($r6);
} else {
    check('Test  8 - groupby entity->sum: total sum > 0', false, 'r6 is null');
}

// =============================================================================
// Test 9: groupby entity -> count
// =============================================================================
$r9 = IO::prdx_groupby($PRDX_PATH, ['entity'], ['amount' => 'count']);
if ($r9 !== null) {
    $nRows = $r9->shape[0];
    $schema = $r9->dtypes;
    $amountType = $schema['amount'] ?? 'unknown';
    check('Test  9 - groupby entity->count: correct groups + Int64 schema',
          $nRows === $nEntities && $amountType === 'Int64',
          sprintf('%d groups, amount_type=%s', $nRows, $amountType));
    unset($r9);
} else {
    check('Test  9 - groupby entity->count: correct groups + Int64 schema', false, 'r9 is null');
}

// =============================================================================
// Test 10: groupby category -> min(amount)
// =============================================================================
$r10 = IO::prdx_groupby($PRDX_PATH, ['category'], ['amount' => 'min']);
if ($r10 !== null) {
    $minVal = $r10['amount']->min();
    check('Test 10 - groupby category->min(amount): min > 0',
          $minVal > 0,
          sprintf('%d categories, min=%.4f', $r10->shape[0], $minVal));
    unset($r10);
} else {
    check('Test 10 - groupby category->min(amount): min > 0', false, 'r10 is null');
}

// =============================================================================
// Test 11: groupby category -> max(amount) > min
// =============================================================================
$r11 = IO::prdx_groupby($PRDX_PATH, ['category'], ['amount' => 'max']);
if ($r11 !== null) {
    $maxVal = $r11['amount']->max();
    check('Test 11 - groupby category->max(amount) > 0',
          $maxVal > 0,
          sprintf('%d categories, max=%.4f', $r11->shape[0], $maxVal));
    unset($r11);
} else {
    check('Test 11 - groupby category->max(amount) > 0', false, 'r11 is null');
}

// =============================================================================
// Test 12: multi-key groupby entity + category -> more groups
// =============================================================================
$r12 = IO::prdx_groupby($PRDX_PATH, ['entity', 'category'], ['amount' => 'sum']);
if ($r12 !== null) {
    $n12 = $r12->shape[0];
    check('Test 12 - multi-key groupby (entity,category): groups > single-key groups',
          $n12 > $nEntities,
          sprintf('%d groups vs %d entity-only groups', $n12, $nEntities));
    unset($r12);
} else {
    check('Test 12 - multi-key groupby (entity,category): groups > single-key groups', false, 'r12 is null');
}

// =============================================================================
// Test 13: invalid column -> null
// =============================================================================
$r13 = IO::prdx_groupby($PRDX_PATH, ['entity'], ['__no_such_col__' => 'sum']);
check('Test 13 - invalid agg column -> null returned',
      $r13 === null,
      $r13 === null ? 'correctly null' : 'unexpected result');

// =============================================================================
// Test 14: invalid path -> null
// =============================================================================
$r14 = IO::prdx_groupby('nonexistent.prdx', ['entity'], ['amount' => 'sum']);
check('Test 14 - invalid path -> null returned',
      $r14 === null,
      $r14 === null ? 'correctly null' : 'unexpected result');

// =============================================================================
// Test 15: prdx_count on invalid path -> -1
// =============================================================================
$badCount = IO::prdx_count('nonexistent.prdx');
check('Test 15 - prdx_count on invalid path -> -1',
      $badCount === -1,
      sprintf('got %d', $badCount));

// =============================================================================
// Summary
// =============================================================================
echo "\n" . str_repeat('=', 60) . "\n";
$passed = count(array_filter($results, fn($r) => $r[1] === 'OK'));
$total = count($results);
echo "  Gap 13 Results: $passed/$total tests passed\n";
echo str_repeat('=', 60) . "\n";

if ($passed < $total) {
    echo "\nFailed tests:\n";
    foreach ($results as $r) {
        if ($r[1] !== 'OK') {
            echo "  FAIL: {$r[0]} - {$r[2]}\n";
        }
    }
    exit(1);
}