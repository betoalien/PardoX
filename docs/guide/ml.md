---
layout: default
title: "ML Integration - Zero-Copy NumPy Bridge"
parent: "User Guide"
nav_order: 6
---

# ML Integration — Zero-Copy NumPy Bridge

PardoX implements the **NumPy `__array__` protocol**, allowing direct zero-copy access from PardoX Series to NumPy arrays. No data is copied — the NumPy array points directly into the Rust HyperBlock buffer.

---

## Zero-Copy NumPy Conversion

```python
import numpy as np
import pardox as px

df = px.read_csv("features.csv")

# Direct pointer into Rust buffer — no data copy
arr = np.array(df["price"])
print(arr.dtype)    # float64
print(arr.shape)    # (50000,)
print(arr.mean())   # identical to df["price"].mean()
```

!!! success "True zero-copy"
    `np.array(df['col'])` invokes the `__array__` dunder, which calls `pardox_get_f64_buffer` — a Rust function that returns a direct pointer into the HyperBlock's `Float64` buffer. NumPy wraps this pointer in an array without allocating new memory.

!!! warning "Lifetime"
    The NumPy array is valid as long as the parent DataFrame is alive. Do not hold onto `arr` after `df` goes out of scope or is freed.

---

## Scikit-Learn Integration

```python
import numpy as np
import pardox as px
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

df = px.read_csv("training_data.csv")
df.cast("quantity", "Float64")
df.fillna(0.0)

# Build feature matrix and target via zero-copy
X = np.column_stack([
    np.array(df["price"]),
    np.array(df["quantity"]),
    np.array(df["discount"]),
])
y = np.array(df["tax"])

# Scale features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Train
model = LinearRegression()
model.fit(X_scaled, y)

print(f"Coefficients: {model.coef_}")
print(f"Intercept:    {model.intercept_:.4f}")
```

---

## PyTorch Integration

```python
import torch
import numpy as np
import pardox as px

df = px.read_csv("embeddings.csv")

# Zero-copy from PardoX → NumPy → PyTorch tensor
arr    = np.array(df["embedding_dim_0"])
tensor = torch.from_numpy(arr)

print(tensor.dtype)    # torch.float64
print(tensor.shape)    # torch.Size([50000])
```

---

## Min-Max Scaling (Built-In)

For normalizing features before ML, PardoX provides a native Min-Max Scaler that runs entirely in Rust:

```python
# Normalize to [0, 1] — returns new DataFrame with 'result_minmax'
normed_df = df.min_max_scale("price")
normed_arr = np.array(normed_df["result_minmax"])

print(normed_arr.min())   # ~0.0
print(normed_arr.max())   # ~1.0
```

This is equivalent to `sklearn.preprocessing.MinMaxScaler` but operates at native Rust speed with no intermediate Python allocations.

---

## Supported Column Types

The zero-copy `__array__` bridge works on `Float64` columns. For `Int64` columns, cast first:

```python
df.cast("quantity", "Float64")
arr = np.array(df["quantity"])   # now Float64
```

`Utf8` (string) columns cannot be zero-copied to NumPy. Use `df.to_dict()` or `df.unique()` to access string data.

---

## No NumPy Import Inside PardoX

PardoX itself does not import NumPy. The `__array__` protocol is implemented entirely through `ctypes` pointer arithmetic. NumPy is only involved at the user call site when `np.array(series)` is invoked.

This means PardoX works in environments where NumPy is not installed, and NumPy integration is opt-in.
