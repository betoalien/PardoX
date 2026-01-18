# Data Mutation & Arithmetic

PardoX is designed for **High-Performance Compute**. Unlike traditional libraries that often copy data during operations, PardoX performs arithmetic and transformations directly on the underlying memory buffers using Rust's safe concurrency and **SIMD** (Single Instruction, Multiple Data) instructions.

---

## 1. Column Selection

You can select a column from a DataFrame using the standard bracket notation. This returns a `Series` object, which is a lightweight wrapper around the Rust column pointer.

```python
# Select a single column
prices = df['price']

# Check the type
print(type(prices))  # <class 'pardox.series.Series'>
```

!!! info "Zero-Copy Access"
    Column selection returns a view, not a copy. Modifications affect the original DataFrame.

---

## 2. Vectorized Arithmetic

PardoX bypasses the Python interpreter for mathematical operations. When you add, subtract, multiply, or divide columns, the instruction is sent to the Rust engine, which processes the arrays in parallel blocks.

### Basic Operations

**Supported operators:** `+`, `-`, `*`, `/`

```python
# Vector vs Vector
# Multiplies 'price' * 'quantity' for all rows instantly
df['total'] = df['price'] * df['quantity']

# Calculate Tax (Vector * Scalar)
# PardoX automatically broadcasts the scalar (0.16) to all rows
df['tax'] = df['total'] * 0.16

# Complex expressions
df['final_total'] = (df['total'] + df['tax']) - df['discount']
```

!!! tip "SIMD Acceleration"
    PardoX uses **AVX2** (on Intel/AMD) and **NEON** (on Apple Silicon) instructions. This means the CPU processes 4 to 8 numbers in a single clock cycle, resulting in speedups of **5x to 20x** compared to pure Python loops.

---

## 3. Creating New Columns

You can assign the result of an operation to a new key in the DataFrame. This allocates new memory in the HyperBlock efficiently.

```python
# Create a boolean flag (stored as 0.0 or 1.0 for now)
# (Comparison operators coming in v0.2)
df['is_expensive'] = df['price'] * 0.0  # Placeholder initialization
```

!!! note "Memory Allocation"
    New columns are allocated efficiently within the Rust HyperBlock, maintaining cache locality.

---

## 4. Data Hygiene (Cleaning)

Real-world data is messy. PardoX provides optimized kernels for cleaning data **in-place** (modifying the existing buffer without creating a copy).

### `fillna(value)`

Fills missing (`NaN` or `null`) values with a specified scalar.

```python
# Replace all nulls in 'age' with 0
df.fillna(0.0) 

# Note: In v0.1, fillna() applies to the entire DataFrame or specific numeric logic.
# Future versions will support column-specific fillna.
```

!!! warning "In-Place Operation"
    No need to reassign: `df.fillna(0.0)` modifies the DataFrame directly.

### `round(decimals)`

Rounds floating-point numbers to a specific precision.

```python
# Round all numeric columns to 2 decimal places
df.round(2)
```

---

## 5. Performance Best Practices

To get the most out of the Mutation Engine:

!!! success "Best Practices"
    - **Chain Operations**: Python handles the operator precedence, but keep expressions vector-based.
    - **Avoid Loops**: Never iterate over rows with a `for` loop (e.g., `for row in df:`). This kills performance. Always use column arithmetic.
    - **In-Place is Faster**: Functions like `fillna()` modify the data directly. You don't need to do `df = df.fillna()`.

### Example: The Right Way vs. The Wrong Way

```python
# ❌ SLOW (Python Loop)
for i in range(len(df)):
    df['new'][i] = df['a'][i] + df['b'][i]

# ✅ FAST (PardoX SIMD)
df['new'] = df['a'] + df['b']
```

!!! danger "Performance Impact"
    The vectorized approach is **100x to 1000x faster** than Python loops for large datasets.