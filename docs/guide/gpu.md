# GPU Acceleration

PardoX v0.3.1 introduces a **GPU Bitonic Sort** pipeline powered by WebGPU compute shaders via the `wgpu` crate. The GPU engine is loaded dynamically from a separate library (`libpardox_gpu.so`), keeping it completely optional.

---

## How It Works

```
df.sort_values("col", gpu=True)
        │
        ▼
   pardox_gpu_sort (Rust FFI)
        │
        ├── Load libpardox_gpu.so dynamically (libloading 0.8)
        ├── Extract Float64 column into contiguous buffer
        ├── Transfer buffer to GPU (VRAM)
        ├── Execute Bitonic Sort compute shader (WebGPU / wgpu 0.19)
        ├── Read sorted buffer back from GPU
        └── Apply row permutation to full DataFrame
        │
        ▼  (if GPU unavailable)
   CPU fallback — parallel Rust merge sort (Rayon)
```

The GPU sort works exclusively on `Float64` columns. The result is applied back to the full DataFrame row order via an **argsort + permutation** step entirely in Rust.

---

## Usage — Python

```python
# CPU sort (default)
sorted_df = df.sort_values("revenue", ascending=True)
sorted_df = df.sort_values("revenue", ascending=False)

# GPU Bitonic sort
sorted_df = df.sort_values("revenue", ascending=True, gpu=True)

print(f"Rows: {sorted_df.shape[0]:,}")
```

---

## Usage — Node.js

```js
// CPU sort
const sortedDf = df.sortValues('revenue', true);

// GPU sort
const sortedDf = df.sortValues('revenue', true, true);  // (by, ascending, gpu)
```

---

## Usage — PHP

```php
// CPU sort
$sorted = $df->sort_values('revenue', true);

// GPU sort
$sorted = $df->sort_values('revenue', true, true);  // ($by, $ascending, $gpu)
```

---

## CPU Fallback

If a compatible GPU is not available or the `wgpu` backend fails to initialize:

1. PardoX logs `[PardoX GPU Sort] GPU not available, using CPU sort.` to stderr.
2. The CPU parallel merge sort (Rayon) is used transparently.
3. The result is identical — only the execution path differs.

This means you can safely call `gpu=True` in all environments without adding error handling.

---

## GPU Requirements

| Requirement | Details |
|-------------|---------|
| API | WebGPU via `wgpu` 0.19 |
| Backends | Vulkan (Linux / Windows), Metal (macOS), DX12 (Windows) |
| VRAM | Minimum ~256 MB for 50k rows (Float64 = 400 KB; permutation overhead ~4 MB) |
| Adapter | Any discrete or integrated GPU with Vulkan / Metal / DX12 support |

!!! note "WebGPU ≠ browser WebGPU"
    PardoX uses the native Rust `wgpu` crate, which targets Vulkan/Metal/DX12 directly. It does not require a browser or WebAssembly environment.

---

## Dynamic Library Loading

The GPU engine is shipped as a **separate shared library** (`libpardox_gpu.so` / `.dylib` / `.dll`) loaded at runtime via `libloading 0.8`. The main `libpardox.so` will function normally even if `libpardox_gpu.so` is absent — GPU calls simply fall back to CPU.

---

## Performance

GPU sort shines for very large `Float64` columns. For small datasets (< 100k rows), the overhead of GPU transfer and shader dispatch may outweigh the benefit.

| Rows | CPU sort | GPU Bitonic sort |
|------|----------|-----------------|
| 50k | ~5ms | ~15ms (transfer overhead dominates) |
| 1M | ~120ms | ~40ms |
| 10M | ~1.4s | ~280ms |

!!! tip "When to use GPU"
    Use `gpu=True` when sorting columns with more than 500,000 rows. For smaller datasets, the default CPU path is faster due to lower overhead.
