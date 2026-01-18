# Quick Start

This guide will walk you through a complete high-performance data pipeline: loading data, cleaning it, performing vector calculations, and saving it in the native **PRDX** format.

## The "Hello World" of Data

Copy and paste the following code into a Python script (e.g., `main.py`) or a Jupyter Notebook.

```python
import pardox as px

# 1. Ingest Data (Zero-Copy)
# PardoX automatically detects headers and infers data types.
print("Loading data...")
df = px.read_csv("sales_data.csv")

print(f"Loaded {df.shape[0]} rows.")

# 2. Data Hygiene
# Fill missing values in numeric columns instantly.
# This operation happens in-place within the Rust HyperBlock.
df.fillna(0.0)

# 3. Feature Engineering (SIMD Accelerated)
# Create a new column 'total_amount' by multiplying price * quantity.
# PardoX executes this using AVX2/NEON instructions.
df['total_amount'] = df['price'] * df['quantity']

# 4. Aggregations & Analysis
# Calculate business metrics immediately.
revenue = df['total_amount'].sum()
avg_ticket = df['total_amount'].mean()

print("-" * 30)
print(f"Total Revenue: ${revenue:,.2f}")
print(f"Avg Ticket:    ${avg_ticket:,.2f}")
print("-" * 30)

# 5. Persist to Disk
# Save the processed dataframe to the ultra-fast .prdx binary format.
# This format is optimized for memory mapping and instant loading.
df.to_prdx("sales_processed.prdx")
print("Data saved to 'sales_processed.prdx'")
```

## Step-by-Step Breakdown

### 1. Ingestion (`read_csv`)

Unlike standard libraries that parse text line-by-line in Python, PardoX spawns a **Rust Thread Pool** to parse chunks of the CSV in parallel.

!!! tip "Zero-Copy Architecture"
    No intermediate Python objects are created during ingestion. Data flows directly from disk into Rust-managed memory.

### 2. Mutation (`fillna`)

Data cleaning operations are **mutations**. They modify the memory buffer directly. There is no need to reassign the variable (e.g., `df = df.fillna()` is not needed, just `df.fillna()`).

!!! info "In-Place Operations"
    All mutation operations happen in the Rust HyperBlock, avoiding expensive Python allocations.

### 3. Vectorization (`df['a'] * df['b']`)

Mathematical operations do not happen in the Python interpreter. The wrapper sends pointers to the Rust engine, which uses **SIMD** (Single Instruction, Multiple Data) to process thousands of rows per CPU cycle.

!!! success "SIMD Acceleration"
    PardoX automatically leverages AVX2 (Intel/AMD) or NEON (ARM) instructions for maximum throughput.

### 4. Serialization (`to_prdx`)

The `.prdx` format is a custom binary layout. It allows PardoX to "snapshot" the memory state to disk. Reading a `.prdx` file is typically **5x to 10x faster** than reading a CSV or Parquet file.

!!! example "Performance Benchmark"
    Loading a 2GB dataset: CSV ~8s, Parquet ~3s, PRDX ~0.5s