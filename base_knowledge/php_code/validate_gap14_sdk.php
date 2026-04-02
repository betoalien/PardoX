#!/usr/bin/env php
<?php
/**
 * validate_gap14_sdk.php - Gap 14: SQL over in-memory DataFrame
 *
 * Tests pardox_sql_query - runs standard SQL SELECT directly on any
 * in-memory DataFrame, coexisting with all existing DataFrame APIs.
 *
 * Datasets:
 *   customers.csv - 50,000 rows
 *   sales.csv    - 100,000 rows
 */

require_once __DIR__ . '/pardox/vendor/autoload.php';

use PardoX\IO;
use PardoX\DataFrame;

$CSV_SALES = 'sales.csv';
$CSV_CUST  = 'customers.csv';

foreach ([$CSV_SALES, $CSV_CUST] as $f) {
    if (!file_exists($f)) {
        fwrite(STDERR, "Missing required file: $f\n");
        exit(1);
    }
}

$results = [];

function check(string $name, bool $cond, string $detail = ''): void {
    global $results;
    $status = $cond ? 'OK' : 'FAIL';
    $results[] = [$name, $status, $detail];
    $mark = $cond ? '✓' : '✗';
    echo "  [$mark] $name" . ($detail ? "  - $detail" : '') . "\n";
}

// =============================================================================
// Load datasets
// =============================================================================
echo "[LOAD] Loading sales.csv (100K rows)...\n";
$sales = IO::read_csv($CSV_SALES);
check('sales.csv loaded', $sales !== null && $sales->shape[0] === 100000,
      sprintf('%d rows', $sales ? $sales->shape[0] : 0));

echo "[LOAD] Loading customers.csv (50K rows)...\n";
$cust = IO::read_csv($CSV_CUST);
check('customers.csv loaded', $cust !== null && $cust->shape[0] === 50000,
      sprintf('%d rows', $cust ? $cust->shape[0] : 0));

echo "\n=== Gap 14: SQL over DataFrames ===\n\n";

// =============================================================================
// Test 1: SELECT *
// =============================================================================
$t0 = microtime(true);
$r1 = $sales->sql('SELECT * FROM t');
$elapsed = microtime(true) - $t0;
$n1 = $r1 ? $r1->shape[0] : 0;
check('Test  1 - SELECT * returns all rows', $n1 === 100000, sprintf('%d rows (%.2fs)', $n1, $elapsed));
unset($r1);

// =============================================================================
// Test 2: SELECT specific columns
// =============================================================================
$r2 = $sales->sql('SELECT id_transaction, price FROM t');
$n2 = $r2 ? $r2->shape[0] : 0;
$cols2 = $r2 ? $r2->columns : [];
check('Test  2 - SELECT id_transaction, price: 2 columns + all rows',
      $n2 === 100000 && in_array('id_transaction', $cols2) && in_array('price', $cols2),
      sprintf('cols=%s rows=%d', implode(',', $cols2), $n2));
unset($r2);

// =============================================================================
// Test 3: SELECT with alias
// =============================================================================
$r3 = $sales->sql('SELECT price AS revenue FROM t');
$cols3 = $r3 ? $r3->columns : [];
check('Test  3 - SELECT price AS revenue: column named "revenue"',
      in_array('revenue', $cols3), sprintf('cols=%s', implode(',', $cols3)));
unset($r3);

// =============================================================================
// Test 4: WHERE numeric
// =============================================================================
$t0 = microtime(true);
$r4 = $sales->sql('SELECT * FROM t WHERE price > 900');
$elapsed = microtime(true) - $t0;
$n4 = $r4 ? $r4->shape[0] : 0;
check('Test  4 - WHERE price > 900: filtered rows > 0 and < total',
      $n4 > 0 && $n4 < 100000, sprintf('%d rows (%.2fs)', $n4, $elapsed));
unset($r4);

// =============================================================================
// Test 5: WHERE string equality
// =============================================================================
$r5 = $sales->sql("SELECT * FROM t WHERE category = 'Toys'");
$n5 = $r5 ? $r5->shape[0] : 0;
check('Test  5 - WHERE category = "Toys": rows > 0', $n5 > 0, sprintf('%d rows', $n5));
unset($r5);

// =============================================================================
// Test 6: WHERE AND
// =============================================================================
$r6 = $sales->sql('SELECT * FROM t WHERE price > 500 AND quantity > 8');
$n6 = $r6 ? $r6->shape[0] : 0;
check('Test  6 - WHERE price > 500 AND quantity > 8: rows > 0', $n6 > 0, sprintf('%d rows', $n6));
unset($r6);

// =============================================================================
// Test 7: WHERE OR
// =============================================================================
$r7 = $sales->sql("SELECT * FROM t WHERE category = 'Toys' OR category = 'Books'");
$n7 = $r7 ? $r7->shape[0] : 0;
check('Test  7 - WHERE category IN Toys OR Books: rows > 0', $n7 > 0, sprintf('%d rows', $n7));
unset($r7);

// =============================================================================
// Test 8: WHERE BETWEEN
// =============================================================================
$r8 = $sales->sql('SELECT * FROM t WHERE price BETWEEN 100 AND 200');
$n8 = $r8 ? $r8->shape[0] : 0;
check('Test  8 - WHERE price BETWEEN 100 AND 200: rows > 0', $n8 > 0, sprintf('%d rows', $n8));
unset($r8);

// =============================================================================
// Test 9: WHERE LIKE
// =============================================================================
$r9 = $sales->sql("SELECT * FROM t WHERE category LIKE 'Home%'");
$n9 = $r9 ? $r9->shape[0] : 0;
check('Test  9 - WHERE category LIKE "Home%": rows > 0', $n9 > 0, sprintf('%d rows', $n9));
unset($r9);

// =============================================================================
// Test 10: WHERE IN
// =============================================================================
$r10 = $cust->sql("SELECT * FROM t WHERE state IN ('CA', 'TX', 'NY')");
$n10 = $r10 ? $r10->shape[0] : 0;
check('Test 10 - WHERE state IN (CA,TX,NY): rows > 0 and < total',
      $n10 > 0 && $n10 < 50000, sprintf('%d rows', $n10));
unset($r10);

// =============================================================================
// Test 11: GROUP BY SUM
// =============================================================================
echo "\n";
$t0 = microtime(true);
$r11 = $sales->sql('SELECT category, SUM(price) AS total FROM t GROUP BY category');
$elapsed = microtime(true) - $t0;
$n11 = $r11 ? $r11->shape[0] : 0;
check('Test 11 - GROUP BY category -> SUM(price): groups > 0',
      $n11 > 0, sprintf('%d groups (%.2fs)', $n11, $elapsed));
unset($r11);

// =============================================================================
// Test 12: GROUP BY COUNT
// =============================================================================
$r12 = $sales->sql('SELECT category, COUNT(price) AS cnt FROM t GROUP BY category');
$n12 = $r12 ? $r12->shape[0] : 0;
$schema12 = $r12 ? $r12->dtypes : [];
$cntType = $schema12['cnt'] ?? 'unknown';
check('Test 12 - GROUP BY category -> COUNT(price): Int64 result',
      $n12 > 0 && $cntType === 'Int64', sprintf('%d groups cnt_type=%s', $n12, $cntType));
unset($r12);

// =============================================================================
// Test 13: ORDER BY ASC
// =============================================================================
$r13 = $sales->sql('SELECT price FROM t ORDER BY price ASC LIMIT 5');
$min13 = $r13 ? $r13['price']->min() : 0;
$isValid = $r13 && $min13 > 0;
check('Test 13 - ORDER BY price ASC LIMIT 5: min price > 0',
      $isValid, sprintf('min=%.2f', $min13));
unset($r13);

// =============================================================================
// Test 14: ORDER BY DESC
// =============================================================================
$r14 = $sales->sql('SELECT price FROM t ORDER BY price DESC LIMIT 5');
$max14 = $r14 ? $r14['price']->max() : 0;
check('Test 14 - ORDER BY price DESC LIMIT 5: max price > 0',
      $max14 > 0, sprintf('max=%.2f', $max14));
unset($r14);

// =============================================================================
// Test 15: LIMIT 10
// =============================================================================
$r15 = $sales->sql('SELECT * FROM t LIMIT 10');
$n15 = $r15 ? $r15->shape[0] : 0;
check('Test 15 - LIMIT 10: exactly 10 rows', $n15 === 10, sprintf('got %d', $n15));
unset($r15);

// =============================================================================
// Test 16: SELECT COUNT(*)
// =============================================================================
$r16 = $sales->sql('SELECT COUNT(*) AS n FROM t');
$n16 = $r16 ? $r16->shape[0] : 0;
$schema16 = $r16 ? $r16->dtypes : [];
$nType = $schema16['n'] ?? 'unknown';
check('Test 16 - SELECT COUNT(*): 1 row, Int64 type',
      $n16 === 1 && $nType === 'Int64', sprintf('rows=%d type=%s', $n16, $nType));
unset($r16);

// =============================================================================
// Test 17: SELECT SUM(price)
// =============================================================================
$r17 = $sales->sql('SELECT SUM(price) AS total FROM t');
$n17 = $r17 ? $r17->shape[0] : 0;
$total17 = $r17 ? $r17['total']->sum() : 0;
check('Test 17 - SELECT SUM(price): 1 row, value > 0',
      $n17 === 1 && $total17 > 0, sprintf('total=%.2f', $total17));
unset($r17);

// =============================================================================
// Test 18: WHERE + ORDER BY + LIMIT
// =============================================================================
$t0 = microtime(true);
$r18 = $sales->sql('SELECT id_transaction, price FROM t WHERE price > 800 ORDER BY price DESC LIMIT 20');
$elapsed = microtime(true) - $t0;
$n18 = $r18 ? $r18->shape[0] : 0;
check('Test 18 - WHERE + ORDER BY DESC + LIMIT 20: 20 rows',
      $n18 === 20, sprintf('(%.2fs) %d rows', $elapsed, $n18));
unset($r18);

// =============================================================================
// Test 19: NOT in WHERE
// =============================================================================
$r5_toys = $sales->sql("SELECT * FROM t WHERE category = 'Toys'");
$n5_toys = $r5_toys ? $r5_toys->shape[0] : 0;
$r19 = $sales->sql("SELECT * FROM t WHERE NOT (category = 'Toys')");
$n19 = $r19 ? $r19->shape[0] : 0;
check('Test 19 - NOT (category = "Toys"): rows + toys rows == total',
      $n19 + $n5_toys === 100000, sprintf('not_toys=%d + toys=%d = %d', $n19, $n5_toys, $n19 + $n5_toys));
unset($r5_toys, $r19);

// =============================================================================
// Test 20: Invalid SQL -> null
// =============================================================================
$r20 = $sales->sql('NOT VALID SQL @@@@');
check('Test 20 - invalid SQL -> null returned', $r20 === null, $r20 === null ? 'correctly null' : 'unexpected result');

// =============================================================================
// Test 21: Unknown column in WHERE -> null
// =============================================================================
$r21 = $sales->sql('SELECT * FROM t WHERE __no_such_col__ > 0');
check('Test 21 - unknown column in WHERE -> null returned',
      $r21 === null, $r21 === null ? 'correctly null' : 'unexpected result');

// =============================================================================
// Summary
// =============================================================================
unset($sales, $cust);

echo "\n" . str_repeat('=', 60) . "\n";
$passed = count(array_filter($results, fn($r) => $r[1] === 'OK'));
$total = count($results);
echo "  Gap 14 Results: $passed/$total tests passed\n";
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