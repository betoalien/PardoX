# PardoX Java SDK

> High-Performance DataFrame Engine for Java/JVM

A universal DataFrame library powered by Rust with zero dependencies. This SDK provides native FFI bindings to the PardoX engine.

## Features

- **Zero Dependencies**: No external drivers or native libraries required
- **Native Performance**: Powered by Rust with SIMD optimization
- **Multi-Database Support**: PostgreSQL, MySQL, SQL Server, MongoDB
- **Format Support**: CSV, JSON, Parquet, .prdx (native binary format)
- **Cross-Platform**: Linux x64, Windows x64, macOS Intel/ARM64

## Installation

### Prerequisites

- Java 17+
- JNA library (included)

### Quick Start

```java
import io.pardox.*;

// Read CSV
DataFrame df = Io.readCsv("data.csv");

// Show data
System.out.println(df.show(10));

// Get shape
int[] shape = df.shape();
System.out.println("Rows: " + shape[0] + ", Columns: " + shape[1]);

// Filter
Series mask = df.get("price").gt(100.0);
DataFrame filtered = df.filter(mask);

// Aggregation
double totalSales = df.get("sales").sum();
double avgPrice = df.get("price").mean();

// Write to PostgreSQL
filtered.toSql("postgresql://user:pass@localhost/db", "table_name");

// Free memory
df.free();
```

## API Reference

### IO Functions

| Function | Description |
|----------|-------------|
| `Io.readCsv(path)` | Read CSV file |
| `Io.readCsv(path, schema)` | Read CSV with schema |
| `Io.readSql(connStr, query)` | Read from PostgreSQL |
| `Io.readMysql(connStr, query)` | Read from MySQL |
| `Io.readSqlServer(connStr, query)` | Read from SQL Server |
| `Io.readMongoDB(connStr, target)` | Read from MongoDB |
| `Io.readJson(jsonString)` | Create DataFrame from JSON |
| `Io.readPrdx(path, limit)` | Read .prdx file preview |
| `DataFrame.fromJsonRecords(data)` | Create from List of Maps |

### DataFrame Operations

| Method | Description |
|--------|-------------|
| `df.shape()` | Get (rows, columns) |
| `df.columns()` | Get column names |
| `df.dtypes()` | Get column types |
| `df.head(n)` | First n rows |
| `df.tail(n)` | Last n rows |
| `df.iloc(start, len)` | Slice by position |
| `df.get(col)` | Get column as Series |
| `df.filter(mask)` | Filter by boolean Series |
| `df.cast(col, type)` | Cast column type |
| `df.join(other, on)` | Hash join |
| `df.add(colA, colB)` | Columnar addition |
| `df.sub(colA, colB)` | Columnar subtraction |
| `df.mul(colA, colB)` | Columnar multiplication |
| `df.sortValues(col, asc)` | Sort by column |
| `df.std(col)` | Standard deviation |
| `df.minMaxScale(col)` | Min-Max normalization |
| `df.fillna(value)` | Fill nulls |
| `df.round(decimals)` | Round numeric columns |

### Export Methods

| Method | Description |
|--------|-------------|
| `df.toCsv(path)` | Export to CSV |
| `df.toPrdx(path)` | Export to .prdx |
| `df.toSql(conn, table)` | Write to PostgreSQL |
| `df.toMysql(conn, table)` | Write to MySQL |
| `df.toSqlServer(conn, table)` | Write to SQL Server |
| `df.toMongoDB(conn, target)` | Write to MongoDB |
| `df.toDict()` | Export as List of Maps |
| `df.toList()` | Export as List of Lists |

### Series Operations

| Method | Description |
|--------|-------------|
| `s.sum()` | Sum |
| `s.mean()` | Mean |
| `s.min()` | Minimum |
| `s.max()` | Maximum |
| `s.count()` | Count |
| `s.std()` | Standard deviation |
| `s.add(other)` | Add series |
| `s.sub(other)` | Subtract series |
| `s.mul(other)` | Multiply series |
| `s.gt(value)` | Greater than filter |
| `s.lt(value)` | Less than filter |
| `s.eq(value)` | Equals filter |

## Compilation

### Compile SDK

```bash
mkdir -p build
javac -source 17 -target 17 -cp "lib/*" -d build pardox/src/io/pardox/*.java
```

### Run Tests

```bash
# Set native library path
java -Djava.library.path=pardox/libs/Linux -cp "build:lib/*" io.pardox.test.BasicTest
```

## Supported Platforms

| Platform | Binary |
|----------|--------|
| Linux x64 | `pardox-cpu-Linux-x64.so` |
| Windows x64 | `pardox-cpu-Windows-x64.dll` |
| macOS Intel | `pardox-cpu-MacOS-Intel.dylib` |
| macOS ARM64 | `pardox-cpu-MacOS-ARM64.dylib` |

## Memory Management

Always free DataFrames when done to prevent memory leaks:

```java
DataFrame df = Io.readCsv("large_file.csv");
// ... use df ...
df.free(); // Release memory
```

Or use try-with-resources pattern with manual cleanup.

## Examples

### Read and Filter CSV

```java
DataFrame df = Io.readCsv("sales.csv");
Series mask = df.get("region").eq("US");
DataFrame usSales = df.filter(mask);
usSales.toCsv("us_sales.csv");
df.free();
```

### Aggregate Operations

```java
DataFrame df = Io.readCsv("orders.csv");
System.out.println("Total: " + df.get("amount").sum());
System.out.println("Average: " + df.get("amount").mean());
System.out.println("Count: " + df.get("id").count());
df.free();
```

### Join DataFrames

```java
DataFrame customers = Io.readCsv("customers.csv");
DataFrame orders = Io.readCsv("orders.csv");
DataFrame joined = customers.join(orders, "customer_id");
joined.toCsv("joined.csv");
customers.free();
orders.free();
joined.free();
```

## Version

v0.3.2

## License

MIT
