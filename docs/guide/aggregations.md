# Aggregations, Analytics & The Observer

PardoX provides two layers of data insight:

1. **Aggregation kernels** — reduce a column to a scalar (sum, mean, std, etc.)
2. **The Observer** — export and inspect the full DataFrame (value_counts, unique, to_dict, to_json)

All operations run entirely in native Rust machine code.

---

## 1. Column Aggregations (Series)

Access aggregation methods directly on any numeric `Series` object (obtained via `df['col']`).

### `sum()`

```python
total_revenue = df['amount'].sum()
print(f"Total: ${total_revenue:,.2f}")
```

**Returns:** `float`

### `mean()`

```python
avg_ticket = df['amount'].mean()
```

**Returns:** `float`

### `min()` / `max()`

```python
highest = df['amount'].max()
lowest  = df['amount'].min()
```

**Returns:** `float`

### `std()`

Sample standard deviation.

```python
volatility = df['amount'].std()
```

**Returns:** `float`

!!! info "Interpretation"
    Low std → values cluster near the mean. High std → values are widely spread.

### `count()`

Number of non-null values in the column.

```python
valid_transactions = df['transaction_id'].count()
```

**Returns:** `int`

---

## 2. DataFrame-Level Standard Deviation

`df.std(col)` is a DataFrame method (distinct from the Series `.std()`) that computes the sample standard deviation for a named column and returns a Python float. Useful when working with derived DataFrames from `df.mul()`, `df.add()`, etc.

```python
revenue_df = df.mul("price", "quantity")
std_val    = revenue_df.std("result_mul")
print(f"Revenue std dev: {std_val:,.4f}")
```

---

## 3. Null Value Handling

All aggregation functions are **null-aware**: they skip `NaN` / null values and compute metrics only over valid data points. This matches SQL and Pandas behavior.

To fill nulls before aggregation:

```python
df.fillna(0.0)
total = df['amount'].sum()
```

---

## 4. Performance

| Method | Mechanism | Speed (10M rows) |
|--------|-----------|-----------------|
| Python `sum(list)` | Iterates `PyObject` one by one | ~1.5s |
| PardoX `.sum()` | SIMD vectorized accumulator | ~0.02s |

!!! tip "Under the hood"
    `.sum()` passes the memory pointer directly to a Rust function. AVX2 instructions add 4–8 values per CPU cycle without ever materializing Python objects.

---

## 5. The Observer — Full DataFrame Export

The Observer module provides functions to export or inspect the entire DataFrame. All string results are **heap-allocated** (proper ownership) and freed after the Python string is created.

### `to_dict()`

Returns all rows as a list of dictionaries (records format). Equivalent to Pandas' `df.to_dict('records')`.

```python
records = df.to_dict()
# [{'price': 19.99, 'quantity': 3, ...}, ...]

print(f"Total records: {len(records)}")
first_row = records[0]
print(first_row['price'])
```

**Returns:** `list[dict]`

### `to_json()`

Returns all rows as a JSON string `"[{...}, ...]"`. Useful for API responses or file writing.

```python
json_str = df.to_json()
with open("output.json", "w") as f:
    f.write(json_str)
```

**Returns:** `str`

### `value_counts(col)`

Returns the frequency of each unique value in a column, sorted by count descending.

```python
state_counts = df.value_counts("state")
# {'TX': 6345, 'CA': 6301, 'CO': 6304, ...}

print(f"Unique states: {len(state_counts)}")

# Top 5
for state, count in list(state_counts.items())[:5]:
    print(f"  {state}: {count:,}")
```

**Returns:** `dict[str, int]`

### `unique(col)`

Returns the unique values of a column in insertion order.

```python
categories = df.unique("category")
# ['Electronics', 'Books', 'Clothing', ...]

print(f"Distinct categories: {len(categories)}")
```

**Returns:** `list`

---

## 6. Observer — PHP and Node.js

### Node.js

```js
// value_counts
const stateCounts = df.valueCounts('state');
console.log(`States: ${Object.keys(stateCounts).length}`);

// unique
const cats = df.unique('category');

// Full export
const records = df.toDict();       // array of objects
const jsonStr = df.toJson();       // JSON string

// tolist — array of arrays (values only)
const matrix = df.tolist();
```

### PHP

```php
// value_counts
$stateCounts = $df->value_counts('state');
echo count($stateCounts) . " unique states\n";

// unique
$cats = $df->unique('category');

// Full export
$records = $df->to_dict();    // array of assoc arrays
$json    = $df->to_json();    // JSON string

// tolist — array of arrays
$matrix  = $df->tolist();
```

---

## 7. Complete Analysis Pipeline

```python
import pardox as px
from pardox.io import execute_sql

df = px.read_csv("sales_50k.csv")
df.cast("quantity", "Float64")

# Feature engineering
revenue_df = df.mul("price", "quantity")

# Aggregations
print(f"Total revenue : ${df['price'].sum():,.2f}")
print(f"Avg price     : ${df['price'].mean():,.4f}")
print(f"Max price     : ${df['price'].max():,.2f}")
print(f"Revenue std   : {revenue_df.std('result_mul'):,.2f}")
print(f"Valid rows    : {df['transaction_id'].count():,}")

# EDA inspection
state_counts  = df.value_counts("state")
categories    = df.unique("category")
print(f"\nTop 3 states: {list(state_counts.items())[:3]}")
print(f"Categories:   {categories}")

# Full export
records = df.to_dict()
print(f"\nExported {len(records):,} records to Python list")
```

---

## 8. GroupBy (Roadmap)

Vectorized `groupby` with a Rust hash-aggregation engine is planned for **v0.3.2**.

```python
# Coming in v0.3.2
summary = df.groupby("region").agg({
    "revenue": "sum",
    "quantity": "mean",
    "transaction_id": "count"
})
```

For now, use `value_counts` for frequency analysis and filter + aggregate on subsets:

```python
# Workaround: filter then aggregate
tx_mask = df['state'].eq("TX")
tx_df   = df.filter(tx_mask)
print(f"TX revenue: ${tx_df['amount'].sum():,.2f}")
```
