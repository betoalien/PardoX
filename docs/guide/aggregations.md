# Aggregations & Analytics

Once data is loaded and cleaned, the next step is extracting insights. PardoX provides a suite of **Analytical Kernels** that compute summary statistics over entire columns instantly.

Unlike iterating through a list in Python (which is slow), PardoX aggregations run entirely in native machine code, leveraging CPU parallelism to scan memory buffers at maximum bandwidth.

---

## 1. Basic Metrics

You can access aggregation methods directly on any **numeric `Series`**.

### Summation (`sum`)
Calculates the total sum of values in the column.

```python
# Calculate Total Revenue
total_revenue = df['amount'].sum()
print(f"Total: ${total_revenue}")
```

### Averages (`mean`)

Calculates the arithmetic mean.

```python
# Calculate Average Transaction Size
avg_ticket = df['amount'].mean()
```

### Extremes (`min` / `max`)

Finds the lowest and highest values in the dataset. Useful for range detection and outlier identification.

```python
highest_sale = df['amount'].max()
lowest_sale  = df['amount'].min()
```

---

## 2. Statistical Analysis

PardoX v0.1 includes kernels for statistical dispersion and counting.

### Standard Deviation (`std`)

Measures the amount of variation or dispersion of a set of values.

!!! info "Interpretation"
    - **Low std**: Values are close to the mean.
    - **High std**: Values are spread out over a wider range.


```python
# Analyze Sales Volatility
volatility = df['amount'].std()
```

### Counting (`count`)

Returns the number of valid (non-null) entries in the column. This is different from the DataFrame shape, as it excludes missing values.

```python
valid_transactions = df['transaction_id'].count()
```

---

## 3. Handling Null Values

PardoX aggregations are **Null-Aware**.

!!! note "Null Handling Strategy"
    - **Skip Strategy**: By default, all aggregation functions ignore null / NaN values. They calculate the metric based only on valid data points.
    - **Consistency**: This matches the behavior of SQL and other major DataFrame libraries.

---

## 4. Performance vs Python

Why use `df['col'].sum()` instead of Python's built-in `sum()`?

| Method | Mechanism | Speed (10M rows) |
|--------|-----------|------------------|
| Python `sum(df['col'])` | Iterates objects one by one | ~1.5s |
| PardoX `.sum()` | SIMD Vectorized Accumulator | ~0.02s |

!!! tip "Under the Hood"
    When you call `.sum()`, PardoX passes the memory pointer to a Rust function that uses **AVX2** instructions to add 4 to 8 numbers simultaneously per CPU cycle, without ever converting the data back to Python objects.

---

## 5. GroupBy Operations (Roadmap)

**Current Status (v0.1 Beta):** PardoX currently supports global aggregations (reducing the entire column to a scalar).

!!! warning "Coming in v0.2"
    We are actively developing the **Split-Apply-Combine** engine to support:
    
    ```python
    # COMING SOON in v0.2
    df.groupby('region').sum('amount')
    ```

!!! tip "Workaround"
    For now, you can filter datasets using boolean masks (coming in v0.1.5) or perform global analysis on subsets.




