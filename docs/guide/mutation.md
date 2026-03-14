---
layout: default
title: "Data Mutation & Arithmetic"
parent: "User Guide"
nav_order: 3
---

# Data Mutation & Arithmetic

PardoX performs all arithmetic and transformations directly on Rust memory buffers using SIMD (Single Instruction, Multiple Data) instructions. No Python objects are created during computation.

---

## 1. Column Selection

```python
# Select a column — returns a Series (lightweight Rust column pointer)
prices = df['price']
print(type(prices))  # <class 'pardox.series.Series'>
```

!!! info "Zero-Copy Access"
    Column selection returns a view into the Rust buffer, not a copy.

---

## 2. Vectorized Arithmetic (Series operators)

Operations via Python operators (`+`, `-`, `*`, `/`) are dispatched to SIMD-accelerated Rust kernels.

```python
# Vector × Vector
df['total']      = df['price'] * df['quantity']

# Vector × Scalar (broadcast)
df['tax']        = df['total'] * 0.08

# Chained expressions
df['net_amount'] = (df['total'] + df['tax']) - df['discount']
```

!!! tip "SIMD acceleration"
    AVX2 (Intel/AMD) and NEON (Apple Silicon) process 4–8 values per CPU cycle — **5x–20x faster** than Python loops.

---

## 3. DataFrame Arithmetic Methods

These methods accept column names as strings and return a **new DataFrame** containing the result column.

### `df.add(col_a, col_b)` — Addition

```python
# Returns new DataFrame with column 'result_add'
sum_df = df.add("price", "tax")
print(sum_df.columns)   # ['result_add']
total = sum_df['result_add'].sum()
```

### `df.sub(col_a, col_b)` — Subtraction

```python
# Returns new DataFrame with column 'result_sub'
profit_df = df.sub("revenue", "cost")
```

### `df.mul(col_a, col_b)` — Multiplication

```python
# Returns new DataFrame with column 'result_mul'
revenue_df = df.mul("price", "quantity")
print(f"Revenue std dev: {revenue_df.std('result_mul'):,.2f}")
```

### `df.std(col)` — Standard Deviation

Returns the **sample standard deviation** of a column as a scalar float. Pure Rust, no NumPy dependency.

```python
std_val = df.std("amount")
print(f"Volatility: {std_val:.4f}")
```

### `df.min_max_scale(col)` — Min-Max Normalization

Normalizes a column to the range `[0, 1]`. Returns a new DataFrame with column `result_minmax`.

```python
# Normalize prices to [0, 1]
normed_df = df.min_max_scale("price")
print(normed_df['result_minmax'].min())   # ~0.0
print(normed_df['result_minmax'].max())   # ~1.0
```

---

## 4. Type Casting

Convert a column to a different type in-place.

```python
# Required before arithmetic when column was inferred as Int64
df.cast("quantity", "Float64")
df.cast("id",       "Utf8")     # convert numeric ID to string
```

**Supported types:** `Int64`, `Float64`, `Utf8`

---

## 5. Sorting (`sort_values`)

Sort the DataFrame by a column. Returns a **new sorted DataFrame**.

```python
# Ascending sort (CPU, Rust parallel merge sort)
sorted_df = df.sort_values("price", ascending=True)

# Descending sort
sorted_df = df.sort_values("price", ascending=False)

# GPU Bitonic sort (falls back to CPU if GPU unavailable)
sorted_df = df.sort_values("price", ascending=True, gpu=True)

print(f"Sorted {sorted_df.shape[0]:,} rows")
```

!!! info "GPU fallback"
    If a GPU is not available or the `wgpu` backend cannot initialize, PardoX automatically uses the CPU sort and logs `[PardoX GPU Sort] GPU not available, using CPU sort.` to stderr. The result is identical.

See [GPU Acceleration](gpu.md) for details on the Bitonic sort pipeline.

---

## 6. Filtering

Apply a boolean Series as a row filter.

```python
# Single condition
mask     = df['price'] > 100.0
filtered = df.filter(mask)

# Combined conditions
mask2    = df['state'].eq("TX")
result   = df.filter(mask).filter(mask2)
```

**Comparison operators on Series:**

| Method | Operator |
|--------|----------|
| `s.eq(val)` | `==` |
| `s.neq(val)` | `!=` |
| `s.gt(val)` | `>` |
| `s.gte(val)` | `>=` |
| `s.lt(val)` | `<` |
| `s.lte(val)` | `<=` |

---

## 7. Data Cleaning

### `fillna(value)`

Fills `NaN` / null values in all numeric columns in-place.

```python
df.fillna(0.0)   # replaces nulls with 0 — modifies buffer directly
```

!!! warning "In-place"
    `fillna` modifies the DataFrame in-place. No need to reassign: `df = df.fillna(0.0)`.

### `round(decimals)`

Rounds all floating-point columns to N decimal places in-place.

```python
df.round(2)   # round all numerics to 2 decimal places
```

---

## 8. Slicing (`iloc`)

Select a range of rows by position.

```python
# Rows 100 to 199 (start inclusive, end exclusive)
subset = df.iloc(100, 200)
print(subset.shape)   # (100, n_cols)
```

---

## 9. Joins

Hash-join two DataFrames on a key column.

```python
# Inner join on the same key column name
result = orders.join(customers, on="customer_id")

# Join on different column names
result = orders.join(customers, left_on="cust_id", right_on="id")
```

---

## 10. Performance Best Practices

!!! success "Do this"
    - Use **column arithmetic** (`df['c'] = df['a'] * df['b']`) for all row-level computations.
    - Cast columns to `Float64` before arithmetic when they were inferred as `Int64`.
    - Chain operations — Python handles precedence, Rust handles execution.

!!! danger "Avoid this"
    - **Python loops** over rows (`for row in df`) destroy performance. Always use column operations.

```python
# ❌ 100x-1000x SLOWER — Python loop
for i in range(len(df)):
    df['new'][i] = df['a'][i] + df['b'][i]

# ✅ FAST — SIMD vectorized
df['new'] = df['a'] + df['b']
```
