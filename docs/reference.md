# API Reference

This document details the classes and functions exposed by the PardoX Python SDK.

---

## Top-Level Functions

### `read_csv`
Reads a Comma Separated Value (CSV) file into a DataFrame using the multi-threaded Rust engine.

```python
def read_csv(path: str, has_headers: bool = True) -> DataFrame
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | `str` | Path to the .csv file. |
| `has_headers` | `bool` | Whether the first row contains column names. Default: `True`. |

**Returns:** `DataFrame`

---

### `read_sql`

Executes a SQL query against a Postgres database and loads the result directly.

```python
def read_sql(connection_string: str, query: str) -> DataFrame
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `connection_string` | `str` | URI format: `postgres://user:pass@host:port/db` |
| `query` | `str` | The SQL SELECT statement to execute. |

**Returns:** `DataFrame`

---

### `read_prdx`

Loads a native PardoX binary file (`.prdx`).

```python
def read_prdx(path: str) -> DataFrame
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | `str` | Path to the .prdx file. |

**Returns:** `DataFrame`

---

### `from_arrow`

Zero-Copy conversion from a PyArrow Table.

```python
def from_arrow(table: pyarrow.Table) -> DataFrame
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `table` | `pyarrow.Table` | PyArrow Table to convert. |

**Returns:** `DataFrame`

---

## Class: `DataFrame`

The main data structure holding the HyperBlock memory manager.

### Properties

- **`shape`**: Returns a tuple `(rows, cols)` representing the dimensions.
- **`columns`**: Returns a list of column names.

### Methods

#### `head(n=5)`

Returns the first `n` rows as a list of dictionaries. Useful for inspection.

```python
df.head(10)  # Show first 10 rows
```

#### `tail(n=5)`

Returns the last `n` rows as a list of dictionaries.

```python
df.tail(10)  # Show last 10 rows
```

#### `to_prdx(path)`

Saves the current DataFrame state to a binary file.

```python
df.to_prdx("output.prdx")
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | `str` | Path to save the .prdx file. |

#### `fillna(value)`

Fills NaN / null values in all compatible columns with the given scalar.

```python
df.fillna(0.0)  # Replace all nulls with 0
```

!!! note "Current Limitation"
    Currently supports filling numeric columns with float values.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `value` | `float` | The value to replace nulls with. |

---

## Class: `Series`

Represents a single column within a DataFrame. Returned when selecting a column (e.g., `df['price']`).

### Arithmetic

**Supported operators:** `+`, `-`, `*`, `/`

Operations are vectorized (SIMD) and return a new Series or modify in-place if assigned back.

```python
# Vector arithmetic
df['total'] = df['price'] * df['quantity']
df['tax'] = df['total'] * 0.16
```

### Aggregations

#### `sum()`

Returns the sum of all values.

```python
total = df['amount'].sum()
```

**Returns:** `float`

#### `mean()`

Returns the arithmetic average.

```python
average = df['amount'].mean()
```

**Returns:** `float`

#### `min()`

Returns the minimum value.

```python
lowest = df['amount'].min()
```

**Returns:** `float`

#### `max()`

Returns the maximum value.

```python
highest = df['amount'].max()
```

**Returns:** `float`

#### `std()`

Returns the standard deviation (population).

```python
volatility = df['amount'].std()
```

**Returns:** `float`

#### `count()`

Returns the count of non-null values.

```python
valid_count = df['amount'].count()
```

**Returns:** `int`

### Transformations

#### `round(decimals)`

Rounds values to the specified number of decimal places in-place.

```python
df['price'].round(2)  # Round to 2 decimals
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `decimals` | `int` | Number of decimal places. |