# Input / Output Operations

The bottleneck in most data pipelines is not calculation, but **IO (Input/Output)**. PardoX solves this by moving data ingestion entirely to the Rust core, bypassing Python's slow file handling and object creation overhead.

---

## 1. CSV Files (Text Data)

PardoX features a **multi-threaded CSV reader**. Instead of reading line-by-line like standard Python libraries, PardoX memory-maps the file and uses parallel workers to parse chunks simultaneously.

### Basic Usage

```python
import pardox as px

# Automatically detects headers and infers schema
df = px.read_csv("dataset.csv")
```

### How it works

!!! info "Intelligent Type Inference"
    The engine scans the first N rows to determine if a column is **Integer**, **Float**, or **String**.

!!! success "Parallel Parsing"
    The file is split into logical blocks, and multiple CPU cores parse them concurrently.

!!! warning "Fail-Fast Error Handling"
    If a row is malformed, PardoX will report the error immediately rather than silently corrupting data.

---

## 2. Native SQL (Database Ingestion)

Load data directly from SQL databases without using Python drivers (like `psycopg2` or `sqlalchemy`) as intermediaries. PardoX connects to the database at the **Rust level**, fetches the binary stream, and constructs the DataFrame in memory.

### Usage

The `read_sql` function requires a standard connection string and a SQL query.

```python
# Format: postgres://user:password@host:port/database
conn_str = "postgres://admin:secret@localhost:5432/analytics_db"

query = """
    SELECT id, amount, date 
    FROM sales 
    WHERE region = 'US-West'
"""

# Executes query and returns PardoX DataFrame
df = px.read_sql(conn_str, query)
```

!!! tip "Performance Note"
    This method is significantly faster than `pandas.read_sql` because it avoids converting SQL types to Python Objects (`PyObject`) before converting them again to internal arrays.

---

## 3. The Native Format (`.prdx`)

The **PRDX format** is the native binary representation of a PardoX HyperBlock. It is designed for instant persistence.

!!! info "Key Features"
    - **No Serialization Overhead**: Unlike CSV or JSON, saving to `.prdx` is effectively a direct memory dump to disk.
    - **Memory Mapping**: Reading a `.prdx` file leverages OS-level memory mapping, allowing near-instant access to data without CPU-intensive parsing.

### Saving to Disk

```python
# Persist the current state of the DataFrame
df.to_prdx("backup_data.prdx")
```

### Loading from Disk

```python
# Load instantly
df_restored = px.read_prdx("backup_data.prdx")
```

!!! success "Benchmark"
    In tests with 10GB datasets, reading a `.prdx` file achieves throughputs of **4.6 GB/s**, limited only by the speed of the NVMe SSD.

---

## 4. Apache Arrow Bridge

PardoX is designed to play well with others. If you have data in **PyArrow**, you can convert it to PardoX with **zero-copy overhead** (passing memory pointers).

```python
import pyarrow as pa
import pardox as px

# Assuming you have a PyArrow Table
arrow_table = pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})

# Convert to PardoX DataFrame
df = px.from_arrow(arrow_table)
```

!!! tip "Interoperability"
    This bridge allows seamless integration with the Arrow ecosystem, including Polars, DuckDB, and Apache Spark.

