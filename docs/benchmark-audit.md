# Independent Benchmark Audit & Reality Check: `BeamExport`

**Audit Conducted By:** Independent Adversarial Engineering Team  
**Target:** `BeamExport` v0.1.0 Benchmark Suite (`bench/export_bench.exs`)  
**Date:** November 2026  

---

## 1. Executive Summary

Previous claims published in documentation and early project notes stated:
- *"Strictly flat $O(1)$ memory consumption across all dataset sizes."*
- *"Zero allocation overhead over baseline streaming."*
- *"50x lower memory under all production workloads."*

Our independent adversarial audit has **debunked the claim of pure $O(1)$ constant memory** as mathematically and architecturally imprecise, identified severe methodological biases in `bench/export_bench.exs`, and formulated technically accurate memory bounds and deployment constraints.

---

## 2. Benchmark Reproduction Results

We reproduced `bench/export_bench.exs` under identical hardware and Erlang/OTP 26 runtime conditions:

| Dataset Size | Approach | Wall Duration | Measured Process Memory | Note |
|---|---|---|---|---|
| **10,000 rows** | Traditional (`Repo.all`) | 108 ms | 5.37 MB | Entire list in memory |
| **10,000 rows** | `BeamExport` (Streaming) | 88 ms | 1.05 MB | Streamed in 500-row chunks |
| **50,000 rows** | Traditional (`Repo.all`) | 485 ms | 26.81 MB | Linear memory scaling |
| **50,000 rows** | `BeamExport` (Streaming) | 392 ms | 1.06 MB | Bounded chunk buffer |
| **100,000 rows** | Traditional (`Repo.all`) | 1,010 ms | 53.58 MB | High GC pressure |
| **100,000 rows** | `BeamExport` (Streaming) | 759 ms | 1.06 MB | Stable process heap |

While `BeamExport` demonstrably maintains a stable process heap when streaming large counts of uniform, small records, the methodology used to claim "50x lower memory" and "$O(1)$ constant memory" contains significant flaws.

---

## 3. Methodological Flaws in the Benchmark Suite

### Flaw 1: Strawman In-Memory Baseline
In `bench/export_bench.exs`:
```elixir
# Traditional Repo.all
bin = NimbleCSV.RFC4180.dump_to_iodata(headers ++ rows) |> IO.iodata_to_binary()
File.write!(out, bin)
```
The benchmark forced the entire dataset through `IO.iodata_to_binary/1`, flattening hundreds of thousands of row binaries into a single, massive contiguous binary block. This represents the absolute worst-case Elixir implementation possible, inflating the perceived memory of the traditional approach. A standard non-streaming pipeline using `File.open` with `IO.binwrite` would consume significantly less memory than `IO.iodata_to_binary`.

### Flaw 2: Measuring Only Process Heap Post-Execution
The benchmark measured `:erlang.process_info(pid, :memory)` immediately before and after the invocation:
```elixir
mem_peak = :erlang.process_info(self(), :memory) |> elem(1)
mem_diff_kb = Float.round((mem_peak - mem_before) / 1024, 2)
```
This metric has several severe limitations:
1. **Off-Heap Binary Leakage:** Binaries larger than 64 bytes (`ProcBin`) are allocated on the shared global Erlang binary heap, not on the process heap. Post-execution process heap sampling under-reports refc binary retention.
2. **Intermediate Allocation Ignored:** It records the heap size after garbage collections may have already reclaimed temporary chunk buffers during the run.
3. **Database Driver Memory Excluded:** Memory allocated in the `DBConnection` socket buffers, OS TCP socket buffers, and ETS tables is completely ignored.

### Flaw 3: Synthetic Uniform Tiny Rows
Every record generated in the benchmark contained only 4 small ASCII fields:
- `id` (integer)
- `name` (~10 bytes)
- `email` (~35 bytes)
- `role` (~6 bytes)

Total row size was approximately **60 bytes**.
In production applications, datasets frequently include:
- Large JSON/JSONB columns (50 KB - 5 MB)
- Text articles, logs, or error stack traces
- Multiline comments and markdown blobs

---

## 4. Disproving the "$O(1)$ Constant Memory" Claim

### Mathematical Reality
Memory in a streaming pipeline is **not** $O(1)$ independent of row size and chunk size.
The true theoretical and empirical memory bound of `BeamExport` is:

$$\text{Memory} = O(\text{chunk\_size} \times \text{max\_row\_size}) + O(\text{buffer\_io})$$

### Empirical Proof
Consider an export where:
- `chunk_size = 1,000`
- Each record has an audited JSON payload of 500 KB.

One single chunk in flight requires:
$$1,000 \times 500\text{ KB} = 500\text{ MB of active binary data}$$

If an engineer configures `chunk_size: 5,000`, the BEAM node will experience a **2.5 GB memory spike**, potentially triggering the Linux OOM Killer. Calling this "$O(1)$ memory" is misleading and dangerous.

---

## 5. Corrected Performance Statements

| Previous Misleading Claim | Corrected Technical Statement |
|---|---|
| *"Strictly flat $O(1)$ memory usage across all dataset sizes."* | **Bounded memory per chunk:** Memory scales with $O(\text{chunk\_size} \times \text{max\_row\_size})$, remaining independent of the *total* number of rows in the dataset. |
| *"Zero-allocation streaming engine."* | **Low-garbage streaming:** Emits nested `iodata` to avoid unnecessary binary concatenation, but memory allocation is proportional to chunk size and column widths. |
| *"Safe under all loads and timeouts."* | **Safe when sized correctly:** Requires configuring `:chunk_size` and `:max_rows` appropriately according to average record size and database pool limits. |

---

## 6. Sizing Recommendations for Production

To prevent memory exhaustion when using `BeamExport` in production:

1. **Calculate Maximum Row Width:**
   Estimate the maximum possible size of a single row.
   $$\text{Max Chunk Memory} = \text{chunk\_size} \times \text{maximum expected row size}$$
2. **Default Sizing:**
   For typical relational records (< 1 KB per row), the default `chunk_size: 500` uses less than 2 MB of transient heap memory.
3. **Wide Tables or Blobs:**
   If exporting tables with large text, JSON, or binary fields (> 100 KB per row), explicitly reduce `:chunk_size` and `:max_rows` to `50` or `100`:
   ```elixir
   BeamExport.to_file(
     {Repo, AuditLog},
     "audit_logs.csv",
     chunk_size: 50,
     max_rows: 50
   )
   ```

