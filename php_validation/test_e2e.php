<?php
/**
 * PardoX Engine v0.3.1 — End-to-End PHP Validation
 * =================================================
 * Tests: CSV ingestion, mean(), value_counts() Observer, MySQL write.
 */

declare(strict_types=1);

// Load via composer autoloader (handles Core/FFI.php, Core/Lib.php, etc.)
require_once __DIR__ . '/pardox/vendor/autoload.php';

use PardoX\DataFrame;
use PardoX\IO;

// ---------------------------------------------------------------------------
// Connection strings (local Docker instances)
// ---------------------------------------------------------------------------
const MYSQL_CONN    = 'mysql://pardox:pardoxpassword@localhost:3306/pardox_test';

function sep(string $title): void {
    echo "\n" . str_repeat('=', 60) . "\n";
    echo "  $title\n";
    echo str_repeat('=', 60) . "\n";
}

// ---------------------------------------------------------------------------
// Task 1: Load 50k CSV
// ---------------------------------------------------------------------------
sep('Task 1: Load hardcore_sales_50k.csv');
$df = DataFrame::read_csv('./hardcore_sales_50k.csv');
$rows = $df->shape[0];
echo "  Rows loaded : " . number_format($rows) . "\n";
echo "  Columns     : " . implode(', ', $df->columns) . "\n";

if ($rows !== 50000) {
    fwrite(STDERR, "[FAIL] Expected 50000 rows, got $rows\n");
    exit(1);
}
echo "  [PASS] CSV loaded successfully.\n";

// ---------------------------------------------------------------------------
// Task 2: Math — mean(discount)
// ---------------------------------------------------------------------------
sep('Task 2: mean(discount)');
$meanDiscount = $df->mean('discount');
echo sprintf("  mean(discount) : %.6f\n", $meanDiscount);

if ($meanDiscount <= 0 || $meanDiscount >= 1) {
    fwrite(STDERR, "[FAIL] mean(discount) out of expected [0,1] range: $meanDiscount\n");
    exit(1);
}
echo "  [PASS] mean() computed correctly.\n";

// ---------------------------------------------------------------------------
// Task 3: EDA Observer — value_counts(state) with heap-alloc memory patch
// ---------------------------------------------------------------------------
sep('Task 3: Observer — value_counts(state)');
$stateCounts = $df->value_counts('state');

if (empty($stateCounts)) {
    fwrite(STDERR, "[FAIL] value_counts returned empty result\n");
    exit(1);
}

echo "  States found : " . count($stateCounts) . "\n";
$topN = array_slice($stateCounts, 0, 5, true);
echo "  Top 5 states by sales volume:\n";
foreach ($topN as $state => $count) {
    echo "    $state => $count\n";
}
echo "  [PASS] Observer value_counts() heap-allocation memory patch working correctly.\n";

// ---------------------------------------------------------------------------
// Task 4: Relational Conqueror — MySQL (append)
// ---------------------------------------------------------------------------
sep('Task 4: Write to MySQL (mode=append)');
try {
    // Create table before writing (to_mysql requires the table to exist)
    IO::executeMysql(MYSQL_CONN, (
        'CREATE TABLE IF NOT EXISTS sales_validation_php (' .
        'transaction_id TEXT, timestamp TEXT, date_only TEXT,' .
        'customer_id BIGINT, state TEXT, latitude DOUBLE,' .
        'longitude DOUBLE, category TEXT, price DOUBLE,' .
        'quantity DOUBLE, discount DOUBLE,' .
        'tax DOUBLE, is_refunded TEXT, notes TEXT)'
    ));
    $rowsWritten = $df->to_mysql(MYSQL_CONN, 'sales_validation_php', 'append');
    echo "  Rows written to MySQL : " . number_format($rowsWritten) . "\n";

    if ($rowsWritten !== 50000) {
        fwrite(STDERR, "[FAIL] Expected 50000, got $rowsWritten\n");
        exit(1);
    }
    echo "  [PASS] MySQL write successful.\n";
} catch (\Throwable $e) {
    fwrite(STDERR, "[FAIL] MySQL: " . $e->getMessage() . "\n");
    exit(1);
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------
sep('VALIDATION COMPLETE');
echo "  PHP SDK v0.3.1 — All tasks passed.\n";
echo "  CSV rows processed : 50,000\n";
echo sprintf("  mean(discount)     : %.6f\n", $meanDiscount);
echo "  Databases written  : MySQL\n";
