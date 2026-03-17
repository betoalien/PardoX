# PardoX Python SDK Documentation

## Installation

```bash
pip install pardox
```

Or install from source:

```bash
cd pardox_project
pip install -e .
```

## Quick Start

```python
import pardox as px

# Create DataFrame from dictionary
df = px.DataFrame([
    {"name": "Alice", "age": 30, "score": 85.5},
    {"name": "Bob", "age": 25, "score": 92.0},
    {"name": "Charlie", "age": 35, "score": 78.5}
])

print(df.head())
```

## API Reference

### DataFrame Creation

#### `px.DataFrame(data)`

Create a DataFrame from a list of dictionaries.

```python
import pardox as px

# From list of dicts
df = px.DataFrame([
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25}
])

# From dict with lists
df = px.DataFrame({
    "name": ["Alice", "Bob", "Charlie"],
    "age": [30, 25, 35]
})
```

### I/O Operations

#### `px.read_csv(path, schema=None, **kwargs)`

Read a CSV file into a DataFrame.

```python
import pardox as px

# Basic CSV reading
df = px.read_csv("data.csv")

# With schema specification
df = px.read_csv("data.csv", schema={
    "id": "Int64",
    "name": "Utf8",
    "amount": "Float64"
})

# With custom delimiter
df = px.read_csv("data.csv", delimiter=";")
```

#### `px.read_sql(conn_string, query)`

Execute a SQL query and return results as DataFrame.

```python
import pardox as px

# PostgreSQL
df = px.read_sql(
    "postgresql://user:password@localhost:5432/mydb",
    "SELECT * FROM sales WHERE amount > 1000"
)
```

#### `px.read_prdx(path)`

Read a PardoX binary file (.prdx).

```python
import pardox as px

df = px.read_prdx("data.prdx")
```

### DataFrame Methods

#### `df.head(n=5)`

Return the first n rows.

```python
# First 10 rows
print(df.head(10))
```

#### `df.tail(n=5)`

Return the last n rows.

```python
# Last 5 rows
print(df.tail())
```

#### `df.shape`

Return the dimensions of the DataFrame (rows, columns).

```python
rows, cols = df.shape
print(f"DataFrame has {rows} rows and {cols} columns")
```

#### `df.dtypes`

Return the data types of each column.

```python
print(df.dtypes)
# Output: {'name': 'Utf8', 'age': 'Int64', 'score': 'Float64'}
```

#### `df.iloc[start:end]`

Slice rows by position.

```python
# Rows 10-20
subset = df.iloc[10:20]
```

#### `df.show(n=10)`

Display an ASCII table representation.

```python
df.show(10)
```

### Column Operations

#### Column Selection

```python
# Select single column (returns Series)
ages = df["age"]

# Select multiple columns
subset = df[["name", "score"]]
```

#### Column Assignment

```python
# Add new column
df["new_col"] = df["age"] * 2

# Or use assign
df = df.assign(new_col=lambda x: x["age"] * 2)
```

### Arithmetic Operations

#### `df.add(col_a, col_b)`

Add two columns.

```python
# Create new column with sum
df = df.add("amount", "tax")
# Result stored in "result_math_add" column
```

#### `df.sub(col_a, col_b)`

Subtract columns.

```python
df = df.sub("price", "discount")
```

#### `df.std(col)`

Calculate standard deviation.

```python
std_value = df.std("score")
```

#### `df.min_max_scale(col)`

Normalize column to [0, 1] range.

```python
df = df.min_max_scale("amount")
```

### Filtering

#### Boolean Filtering

```python
# Filter by condition
filtered = df[df["age"] > 25]

# Multiple conditions
filtered = df[(df["age"] > 25) & (df["score"] > 80)]
```

### Aggregations

Available on Series objects:

```python
# Column as Series
col = df["score"]

# Aggregations
total = col.sum()
average = col.mean()
maximum = col.max()
minimum = col.min()
count = col.count()
std_dev = col.std()
```

### Sorting

#### `df.sort_values(by, ascending=True, gpu=False)`

Sort DataFrame by column values.

```python
# Sort by name ascending
df_sorted = df.sort_values("name")

# Sort by amount descending
df_sorted = df.sort_values("amount", ascending=False)

# Use GPU for large datasets
df_sorted = df.sort_values("amount", gpu=True)
```

### Joins

#### `df.join(other, on, how='inner')`

Join with another DataFrame.

```python
# Inner join on client_id
result = df.join(
    other=clients_df,
    on="client_id"
)

# Left join
result = df.join(
    other=clients_df,
    on="client_id",
    how="left"
)
```

### Data Cleaning

#### `df.fillna(value)`

Fill null values.

```python
# Fill numeric columns with 0
df = df.fillna(0.0)

# Fill with specific value
df = df.fillna({"score": 50.0, "name": "Unknown"})
```

#### `df.round(decimals)`

Round numeric columns.

```python
# Round to 2 decimal places
df = df.round(2)
```

### Type Casting

#### `df.cast(column, target_type)`

Convert column to different type.

```python
# Convert string to integer
df = df.cast("age", "Int64")

# Convert to float
df = df.cast("price", "Float64")
```

### Export

#### `df.to_csv(path, **kwargs)`

Export to CSV.

```python
df.to_csv("output.csv")

# With options
df.to_csv("output.csv", delimiter=";")
```

#### `df.to_prdx(path)`

Export to PardoX binary format.

```python
df.to_prdx("data.prdx")
```

#### `df.to_sql(conn_string, table, mode='append', conflict_cols=None)`

Write to SQL table.

```python
# Append to table
df.to_sql(
    "postgresql://user:pass@localhost/mydb",
    "sales",
    mode="append"
)

# Upsert with conflict handling
df.to_sql(
    "postgresql://user:pass@localhost/mydb",
    "sales",
    mode="upsert",
    conflict_cols=["id"]
)
```

### Series API

The Series class represents a single column:

```python
s = df["name"]

# Arithmetic
s2 = s + " (copy)"

# Comparisons (return boolean Series)
mask = s == "Alice"

# String operations
upper = s.upper()
lower = s.lower()
```

## Database Support

### PostgreSQL

```python
import pardox as px

# Read from PostgreSQL
df = px.read_sql(
    "postgresql://user:password@host:5432/db",
    "SELECT * FROM table"
)

# Write to PostgreSQL
df.to_sql(
    "postgresql://user:password@host:5432/db",
    "table_name",
    mode="append"  # or "upsert"
)
```

### MySQL

```python
# Read from MySQL
df = px.read_mysql(
    "mysql://user:password@host:3306/db",
    "SELECT * FROM table"
)

# Write to MySQL
df.to_mysql(
    "mysql://user:password@host:3306/db",
    "table_name",
    mode="append"
)
```

### SQL Server

```python
# Read from SQL Server
df = px.read_sqlserver(
    "Server=host;Database=db;User Id=user;Password=pass;",
    "SELECT * FROM table"
)

# Write to SQL Server
df.to_sqlserver(
    "Server=host;Database=db;User Id=user;Password=pass;",
    "table_name"
)
```

### MongoDB

```python
# Read from MongoDB
df = px.read_mongodb(
    "mongodb://user:password@host:27017",
    "database.collection"
)

# Write to MongoDB
df.to_mongodb(
    "mongodb://user:password@host:27017",
    "database.collection",
    mode="append"  # or "replace"
)
```

## Performance Tips

1. **Use `.prdx` format** for repeated reads - it's much faster than CSV
2. **Use GPU sorting** for large datasets: `df.sort_values(col, gpu=True)`
3. **Batch SQL writes**: Use `mode="append"` for bulk inserts
4. **Use `fillna()` before computations** to handle missing values

## Error Handling

```python
import pardox as px

try:
    df = px.read_csv("nonexistent.csv")
except FileNotFoundError as e:
    print(f"File not found: {e}")
except Exception as e:
    print(f"Error: {e}")
```

## Binary Format (.prdx)

PardoX's native binary format provides:
- ~4.6 GB/s read throughput
- Columnar storage (HyperBlock)
- Automatic compression

```python
# Write
df.to_prdx("data.prdx")

# Read (much faster than CSV)
df = px.read_prdx("data.prdx")
```
