# PardoX PHP SDK Documentation

## Installation

```bash
composer require pardox/pardox-php
```

Or install manually:

```bash
git clone https://github.com/your-repo/pardox-php.git
cd pardox-php
composer install
```

## Requirements

- PHP 8.1 or higher
- FFI extension enabled (`ffi.enable=1` in php.ini)

## Quick Start

```php
<?php

require_once __DIR__ . '/vendor/autoload.php';

use PardoX\DataFrame;
use PardoX\IO;

// Create DataFrame from array
$data = [
    ["name" => "Alice", "age" => 30, "score" => 85.5],
    ["name" => "Bob", "age" => 25, "score" => 92.0],
    ["name" => "Charlie", "age" => 35, "score" => 78.5]
];

$df = new DataFrame($data);

echo $df->show();
```

## API Reference

### DataFrame Creation

#### `new DataFrame(array $data)`

Create a DataFrame from an array of associative arrays.

```php
<?php

use PardoX\DataFrame;

// From array of associative arrays
$df = new DataFrame([
    ["name" => "Alice", "age" => 30],
    ["name" => "Bob", "age" => 25]
]);

// Empty DataFrame
$df = new DataFrame();
```

### I/O Operations

#### `DataFrame::read_csv(string $path, ?array $schema = null): self`

Read a CSV file into a DataFrame.

```php
<?php

use PardoX\DataFrame;

// Basic CSV reading
$df = DataFrame::read_csv("data.csv");

// With schema specification
$df = DataFrame::read_csv("data.csv", [
    "id" => "Int64",
    "name" => "Utf8",
    "amount" => "Float64"
]);
```

#### `DataFrame::read_sql(string $connString, string $query): self`

Execute a SQL query and return results as DataFrame.

```php
<?php

use PardoX\DataFrame;

// PostgreSQL
$df = DataFrame::read_sql(
    "postgresql://user:password@localhost:5432/mydb",
    "SELECT * FROM sales WHERE amount > 1000"
);
```

#### `IO::read_prdx(string $path): array`

Read a PardoX binary file (.prdx).

```php
<?php

use PardoX\IO;

$data = IO::read_prdx("data.prdx");
```

### DataFrame Methods

#### `$df->head(int $n = 5): DataFrame`

Return the first n rows.

```php
<?php

// First 10 rows
$head = $df->head(10);
echo $head->show();
```

#### `$df->tail(int $n = 5): DataFrame`

Return the last n rows.

```php
<?php

$tail = $df->tail(5);
```

#### `$df->shape: array`

Return the dimensions of the DataFrame.

```php
<?php

$shape = $df->shape;  // [rows, cols]
echo "Rows: {$shape[0]}, Cols: {$shape[1]}";
```

#### `$df->columns: array`

Return the list of column names.

```php
<?php

$columns = $df->columns;
print_r($columns);
```

#### `$df->dtypes: array`

Return the data types of each column.

```php
<?php

$types = $df->dtypes;
// ['name' => 'Utf8', 'age' => 'Int64', 'score' => 'Float64']
print_r($types);
```

#### `$df->iloc(int $start, int $end): DataFrame`

Slice rows by position.

```php
<?php

// Rows 10-20
$subset = $df->iloc(10, 20);
```

#### `$df->show(int $n = 10): string`

Display an ASCII table representation.

```php
<?php

echo $df->show(10);
```

### Column Operations

#### Column Selection

```php
<?php

// Select single column (returns Series)
$ages = $df["age"];

// Using ArrayAccess
$names = $df->offsetGet("name");
```

#### Column Assignment

```php
<?php

// Add new column
$df["new_col"] = $df["age"] * 2;
```

### Arithmetic Operations

#### `$df->add(string $colA, string $colB): DataFrame`

Add two columns.

```php
<?php

// Create new column with sum
$df = $df->add("amount", "tax");
// Result stored in "result_math_add" column
```

#### `$df->sub(string $colA, string $colB): DataFrame`

Subtract columns.

```php
<?php

$df = $df->sub("price", "discount");
```

#### `$df->std(string $col): float`

Calculate standard deviation.

```php
<?php

$stdValue = $df->std("score");
```

#### `$df->minMaxScale(string $col): DataFrame`

Normalize column to [0, 1] range.

```php
<?php

$df = $df->minMaxScale("amount");
```

### Filtering

#### `$df->filter(Series $mask): DataFrame`

Filter using a boolean Series.

```php
<?php

// Create mask using Series comparison
$mask = $df["age"]->gt(25);
$filtered = $df->filter($mask);
```

### Aggregations

Available on Series objects:

```php
<?php

// Column as Series
$col = $df["score"];

// Aggregations
$total = $col->sum();
$average = $col->mean();
$maximum = $col->max();
$minimum = $col->min();
$count = $col->count();
$stdDev = $col->std();
```

### Sorting

#### `$df->sortValues(string $by, bool $ascending = true, bool $gpu = false): DataFrame`

Sort DataFrame by column values.

```php
<?php

// Sort by name ascending
$sorted = $df->sortValues("name");

// Sort by amount descending
$sorted = $df->sortValues("amount", false);

// Use GPU for large datasets
$sorted = $df->sortValues("amount", false, true);
```

### Joins

#### `$df->join(DataFrame $other, array $options): DataFrame`

Join with another DataFrame.

```php
<?php

// Inner join on client_id
$result = $df->join($clients_df, [
    "on" => "client_id"
]);

// With different column names
$result = $df->join($clients_df, [
    "leftOn" => "customer_id",
    "rightOn" => "id"
]);
```

### Data Cleaning

#### `$df->fillna(float $value): self`

Fill null values.

```php
<?php

// Fill numeric columns with 0
$df->fillna(0.0);
```

#### `$df->round(int $decimals): self`

Round numeric columns.

```php
<?php

// Round to 2 decimal places
$df->round(2);
```

### Type Casting

#### `$df->cast(string $column, string $targetType): self`

Convert column to different type.

```php
<?php

// Convert string to integer
$df->cast("age", "Int64");

// Convert to float
$df->cast("price", "Float64");
```

### Export

#### `$df->to_csv(string $path): bool`

Export to CSV.

```php
<?php

$df->to_csv("output.csv");
```

#### `$df->to_prdx(string $path): bool`

Export to PardoX binary format.

```php
<?php

$df->to_prdx("data.prdx");
```

#### `$df->to_sql(string $connString, string $table, string $mode = 'append', array $conflictCols = []): int`

Write to SQL table.

```php
<?php

// Append to table
$rows = $df->to_sql(
    "postgresql://user:pass@localhost/mydb",
    "sales",
    "append"
);

// Upsert with conflict handling
$rows = $df->to_sql(
    "postgresql://user:pass@localhost/mydb",
    "sales",
    "upsert",
    ["id"]
);
```

## Series API

The Series class represents a single column:

```php
<?php

use PardoX\Series;

$s = $df["name"];

// Arithmetic
$s2 = $s->add($s);  // vectorized add
$s2 = $s->sub($s);  // vectorized sub
$s2 = $s->mul(2);   // multiply by scalar
$s2 = $s->div(2);   // divide by scalar

// Comparisons (return boolean Series)
$mask = $s->eq("Alice");
$mask = $s->neq("Bob");
$mask = $s->gt(25);
$mask = $s->gte(18);
$mask = $s->lt(65);
$mask = $s->lte(100);

// Aggregations
$sum = $s->sum();
$mean = $s->mean();
$min = $s->min();
$max = $s->max();
$count = $s->count();
$std = $s->std();
```

## Database Support

### PostgreSQL

```php
<?php

use PardoX\DataFrame;

// Read from PostgreSQL
$df = DataFrame::read_sql(
    "postgresql://user:password@host:5432/db",
    "SELECT * FROM table"
);

// Write to PostgreSQL
$df->to_sql(
    "postgresql://user:password@host:5432/db",
    "table_name",
    "append"  // or "upsert"
);
```

### MySQL

```php
<?php

use PardoX\DataFrame;

// Read from MySQL
$df = DataFrame::read_mysql(
    "mysql://user:password@host:3306/db",
    "SELECT * FROM table"
);

// Write to MySQL
$df->to_mysql(
    "mysql://user:password@host:3306/db",
    "table_name",
    "append"  // or "replace", "upsert"
);
```

### SQL Server

```php
<?php

use PardoX\DataFrame;

// Read from SQL Server
$df = DataFrame::read_sqlserver(
    "Server=host;Database=db;User Id=user;Password=pass;",
    "SELECT * FROM table"
);

// Write to SQL Server
$df->to_sqlserver(
    "Server=host;Database=db;User Id=user;Password=pass;",
    "table_name",
    "append"
);
```

### MongoDB

```php
<?php

use PardoX\DataFrame;

// Read from MongoDB
$df = DataFrame::read_mongodb(
    "mongodb://user:password@host:27017",
    "database.collection"
);

// Write to MongoDB
$df->to_mongodb(
    "mongodb://user:password@host:27017",
    "database.collection",
    "append"  // or "replace"
);
```

## Observers / Export Methods

### `$df->to_json(): string`

Serialize to JSON string.

```php
<?php

$json = $df->to_json();
```

### `$df->to_dict(): array`

Convert to array of objects.

```php
<?php

$arr = $df->to_dict();
// [["name"=>"Alice", "age"=>30], ...]
```

### `$df->tolist(): array`

Convert to array of arrays.

```php
<?php

$arr = $df->tolist();
// [["Alice", 30], ["Bob", 25], ...]
```

### `$df->value_counts(string $col): array`

Get frequency of unique values.

```php
<?php

$counts = $df->value_counts("category");
// ["Electronics" => 150, "Clothing" => 100, ...]
```

### `$df->unique(string $col): array`

Get unique values.

```php
<?php

$unique = $df->unique("name");
// ["Alice", "Bob", "Charlie", ...]
```

## Error Handling

```php
<?php

use PardoX\DataFrame;

try {
    $df = DataFrame::read_csv("nonexistent.csv");
} catch (RuntimeException $e) {
    echo "Error: " . $e->getMessage();
}

// Check if DataFrame is empty
if ($df->shape[0] === 0) {
    echo "DataFrame is empty";
}
```

## Binary Format (.prdx)

PardoX's native binary format provides:
- ~4.6 GB/s read throughput
- Columnar storage (HyperBlock)
- Automatic compression

```php
<?php

use PardoX\DataFrame;
use PardoX\IO;

// Write
$df->to_prdx("data.prdx");

// Read (much faster than CSV)
$data = IO::read_prdx("data.prdx");
$df = new DataFrame($data);
```

## Performance Tips

1. **Use `.prdx` format** for repeated reads
2. **Use GPU sorting** for large datasets: `sortValues("col", false, true)`
3. **Batch SQL writes** with `mode="append"`
4. **Use `fillna()` before computations**
5. **Enable FFI caching** in production:

```php
// In your bootstrap
if (function_exists('opcache_get_status')) {
    opcache_compile_file(__DIR__ . '/vendor/autoload.php');
}
```
