# PardoX JavaScript/Node.js SDK Documentation

## Installation

```bash
npm install @pardox/pardox
```

Or install from source:

```bash
git clone https://github.com/your-repo/pardox-js.git
cd pardox-js
npm install
```

## Requirements

- Node.js 18 or higher
- koffi (`npm install koffi`)

## Quick Start

```javascript
const { DataFrame } = require('@pardox/pardox');

// Create DataFrame from array
const df = DataFrame.fromArray([
    { name: "Alice", age: 30, score: 85.5 },
    { name: "Bob", age: 25, score: 92.0 },
    { name: "Charlie", age: 35, score: 78.5 }
]);

df.show();
```

## API Reference

### DataFrame Creation

#### `DataFrame.fromArray(data)`

Create a DataFrame from an array of objects.

```javascript
const { DataFrame } = require('@pardox/pardox');

// From array of objects
const df = DataFrame.fromArray([
    { name: "Alice", age: 30 },
    { name: "Bob", age: 25 }
]);

// Empty DataFrame
const df = new DataFrame();
```

### I/O Operations

#### `DataFrame.read_csv(path, schema?)`

Read a CSV file into a DataFrame.

```javascript
const { DataFrame } = require('@pardox/pardox');

// Basic CSV reading
const df = DataFrame.read_csv('./data.csv');

// With schema specification
const df = DataFrame.read_csv('./data.csv', {
    id: 'Int64',
    name: 'Utf8',
    amount: 'Float64'
});
```

#### `DataFrame.read_sql(connectionString, query)`

Execute a SQL query and return results as DataFrame.

```javascript
const { DataFrame } = require('@pardox/pardox');

// PostgreSQL
const df = DataFrame.read_sql(
    'postgresql://user:password@localhost:5432/mydb',
    'SELECT * FROM sales WHERE amount > 1000'
);
```

#### `DataFrame.read_mysql(connectionString, query)`

Read from MySQL.

```javascript
const { DataFrame } = require('@pardox/pardox');

const df = DataFrame.read_mysql(
    'mysql://user:password@localhost:3306/mydb',
    'SELECT * FROM users'
);
```

#### `DataFrame.read_sqlserver(connectionString, query)`

Read from SQL Server.

```javascript
const { DataFrame } = require('@pardox/pardox');

const df = DataFrame.read_sqlserver(
    'Server=localhost;Database=mydb;User Id=user;Password=pass;',
    'SELECT * FROM orders'
);
```

#### `DataFrame.read_mongodb(connectionString, dbDotCollection)`

Read from MongoDB.

```javascript
const { DataFrame } = require('@pardox/pardox');

const df = DataFrame.read_mongodb(
    'mongodb://user:password@localhost:27017',
    'mydb.collection'
);
```

### DataFrame Methods

#### `df.head(n=5)`

Return the first n rows.

```javascript
// First 10 rows
const head = df.head(10);
```

#### `df.tail(n=5)`

Return the last n rows.

```javascript
const tail = df.tail(5);
```

#### `df.shape`

Return the dimensions of the DataFrame.

```javascript
const [rows, cols] = df.shape;
console.log(`Rows: ${rows}, Cols: ${cols}`);
```

#### `df.columns`

Return the list of column names.

```javascript
const cols = df.columns;
console.log(cols); // ['name', 'age', 'score']
```

#### `df.dtypes`

Return the data types of each column.

```javascript
const types = df.dtypes;
// { name: 'Utf8', age: 'Int64', score: 'Float64' }
```

#### `df.iloc(start, end)`

Slice rows by position.

```javascript
// Rows 10-20
const subset = df.iloc(10, 20);
```

#### `df.show(n=10)`

Display an ASCII table representation.

```javascript
df.show(10);
```

### Column Operations

#### Column Selection (Proxy-based)

```javascript
// Select single column (returns Series)
const ages = df.age;
const names = df['name'];

// Using method
const col = df.column('age');
```

#### Column Assignment

```javascript
// Add new column
df.newCol = df.age.map(x => x * 2);

// Or using setColumn
df._setColumn('double_age', df.age.mul(2));
```

### Arithmetic Operations

#### `df.add(colA, colB)`

Add two columns.

```javascript
// Create new column with sum
const result = df.add('amount', 'tax');
// Result stored in "result_math_add" column
```

#### `df.sub(colA, colB)`

Subtract columns.

```javascript
const result = df.sub('price', 'discount');
```

#### `df.std(col)`

Calculate standard deviation.

```javascript
const stdValue = df.std('score');
```

#### `df.minMaxScale(col)`

Normalize column to [0, 1] range.

```javascript
const scaled = df.minMaxScale('amount');
```

### Filtering

#### Boolean Filtering

```javascript
// Filter using a mask Series
const mask = df.age.gt(25);
const filtered = df.filter(mask);

// Direct comparison returns a mask
const filtered2 = df.filter(df.amount.gt(1000));
```

### Aggregations

Available on Series objects:

```javascript
const col = df.score;

// Aggregations
const total = col.sum();
const average = col.mean();
const maximum = col.max();
const minimum = col.min();
const count = col.count();
const stdDev = col.std();
```

### Sorting

#### `df.sortValues(by, ascending=true, gpu=false)`

Sort DataFrame by column values.

```javascript
// Sort by name ascending
const sorted = df.sortValues('name');

// Sort by amount descending
const sorted = df.sortValues('amount', false);

// Use GPU for large datasets
const sorted = df.sortValues('amount', false, true);
```

### Joins

#### `df.join(other, { on, leftOn, rightOn })`

Join with another DataFrame.

```javascript
// Inner join on client_id
const result = df.join(clients_df, { on: 'client_id' });

// With different column names
const result = df.join(clients_df, {
    leftOn: 'customer_id',
    rightOn: 'id'
});
```

### Data Cleaning

#### `df.fillna(val)`

Fill null values.

```javascript
df.fillna(0.0);
```

#### `df.round(decimals)`

Round numeric columns.

```javascript
df.round(2);
```

### Type Casting

#### `df.cast(col, targetType)`

Convert column to different type.

```javascript
df.cast('age', 'Int64');
df.cast('price', 'Float64');
```

### Export

#### `df.toCsv(path)`

Export to CSV.

```javascript
df.toCsv('./output.csv');
```

#### `df.toPrdx(path)`

Export to PardoX binary format.

```javascript
df.toPrdx('./data.prdx');
```

#### `df.toSql(connectionString, table, mode, conflictCols)`

Write to SQL table.

```javascript
// Append to table
df.toSql('postgresql://user:pass@localhost/mydb', 'sales', 'append');

// Upsert with conflict handling
df.toSql('postgresql://user:pass@localhost/mydb', 'sales', 'upsert', ['id']);
```

#### `df.toMysql(connectionString, table, mode, conflictCols)`

Write to MySQL table.

```javascript
df.toMysql('mysql://user:pass@localhost/mydb', 'users', 'append');
```

#### `df.toSqlserver(connectionString, table, mode, conflictCols)`

Write to SQL Server.

```javascript
df.toSqlserver('Server=localhost;Database=mydb;User Id=user;Password=pass;', 'orders');
```

#### `df.toMongodb(connectionString, dbDotCollection, mode)`

Write to MongoDB.

```javascript
df.toMongodb('mongodb://user:pass@localhost:27017', 'mydb.collection', 'append');
```

## Series API

The Series class represents a single column:

```javascript
const s = df.name;

// Arithmetic
const s2 = s.add(s);        // vectorized add
const s2 = s.sub(s);        // vectorized sub
const s2 = s.mul(2);        // multiply by scalar
const s2 = s.div(2);        // divide by scalar
const s2 = s.mod(2);        // modulo

// Comparisons (return boolean Series)
const mask = s.eq('Alice');
const mask = s.neq('Bob');
const mask = s.gt(25);
const mask = s.gte(18);
const mask = s.lt(65);
const mask = s.lte(100);

// Aggregations
const sum = s.sum();
const mean = s.mean();
const min = s.min();
const max = s.max();
const count = s.count();
const std = s.std();
```

## Observer / Export Methods

### `df.toJson(limit?)`

Serialize to JSON string.

```javascript
const json = df.toJson(100);
```

### `df.toDict()`

Convert to array of objects.

```javascript
const arr = df.toDict();
// [{ name: 'Alice', age: 30 }, ...]
```

### `df.toList()`

Convert to array of arrays.

```javascript
const arr = df.toList();
// [['Alice', 30], ['Bob', 25], ...]
```

### `df.valueCounts(col)`

Get frequency of unique values.

```javascript
const counts = df.valueCounts('category');
// { Electronics: 150, Clothing: 100, ... }
```

### `df.unique(col)`

Get unique values.

```javascript
const unique = df.unique('name');
// ['Alice', 'Bob', 'Charlie', ...]
```

## Functional API

The SDK also provides functional-style I/O helpers:

```javascript
const { read_csv, read_sql, read_mysql, read_sqlserver, read_mongodb } = require('@pardox/pardox');

// Read CSV
const df = read_csv('./data.csv');

// Read from SQL
const df = read_sql('postgresql://...', 'SELECT * FROM users');

// Execute SQL (returns row count)
const { executeSql } = require('@pardox/pardox');
const count = executeSql('postgresql://...', 'INSERT INTO ...');
```

## Error Handling

```javascript
const { DataFrame } = require('@pardox/pardox');

try {
    const df = DataFrame.read_csv('./nonexistent.csv');
} catch (e) {
    console.error('Error:', e.message);
}

// Check if DataFrame is empty
if (df.shape[0] === 0) {
    console.log('DataFrame is empty');
}
```

## Memory Management

The DataFrame manages Rust memory automatically. However, you can explicitly free memory:

```javascript
const df = DataFrame.read_csv('./large_file.csv');

// Do operations...

// Free memory when done (optional - GC will also free)
df._free();
```

## Binary Format (.prdx)

PardoX's native binary format provides:
- ~4.6 GB/s read throughput
- Columnar storage (HyperBlock)
- Automatic compression

```javascript
const { DataFrame } = require('@pardox/pardox');

// Write
df.toPrdx('./data.prdx');

// Read (much faster than CSV)
const df = DataFrame.read_prdx('./data.prdx');
```

## Performance Tips

1. **Use `.prdx` format** for repeated reads - it's much faster than CSV
2. **Use GPU sorting** for large datasets: `df.sortValues(col, false, true)`
3. **Batch SQL writes** with `toSql(conn, table, 'append')`
4. **Use `fillna()` before computations** to handle missing values
5. **Reuse DataFrames** when possible instead of creating new ones

## TypeScript Support

The SDK includes TypeScript definitions. Import with types:

```typescript
import { DataFrame, Series } from '@pardox/pardox';

const df: DataFrame = DataFrame.read_csv('./data.csv');
const col: Series = df.name;
const count: number = col.count();
```

## Platform Support

The SDK automatically detects your platform and loads the correct binary:

- Linux x64 (`libpardox.so`)
- macOS Intel (`libpardox.dylib`)
- macOS Apple Silicon (`libpardox.dylib`)
- Windows x64 (`pardox.dll`)

Binaries are located in `./libs/` directory.
