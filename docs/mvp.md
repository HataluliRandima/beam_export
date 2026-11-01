# Minimum Viable Product (MVP) Specification

**Status:** Approved  
**Author:** Core Architecture Team  
**Date:** November 2026  
**Project:** `beam_export`  

---

## 1. Product Vision & Refined Goal

The original mission statement:
> *"Stream a large Ecto dataset into CSV without loading the entire dataset into memory."*

Following our ecosystem research and ADR 001 architecture review, we refine this goal:

> **"Provide a production-grade, memory-bounded export engine for Elixir that streams arbitrary enumerable datasets and Ecto queries into formatted CSV files and HTTP responses, prioritizing database connection safety, deterministic resource teardown, and operational observability."**

---

## 2. In Scope for MVP

The MVP consists of the core streaming engine and the minimal set of integrations required to solve production data exports cleanly.

### 2.1 Core Streaming & Pipeline Architecture
* **Source Agnostic:** Accepts any Elixir `Enumerable` (e.g. lists, `Stream`, custom producers) or an Ecto query + repository specification.
* **Lazy Row Transformation:** Pluggable row mapper function (`record -> [field_values]`) that executes lazily per item without intermediate list allocations.
* **Header Support:** Automatic header row prepending (static list or dynamically generated).
* **Deterministic Resource Teardown:** Guarantees that open file descriptors, database transactions, and temporary files are closed/deleted even when the consumer halts early, crashes, or disconnects.

### 2.2 Incremental CSV Serialization
* **RFC 4180 Compliance:** Proper quoting and escaping of commas, quotes (`"` escaped as `""`), newlines (`\r\n` or `\n`), and special characters.
* **`iodata` / `io_list` Output:** Generates nested iodata chunks directly to minimize memory allocations and garbage collection overhead.
* **High Performance Engine:** Built on top of `NimbleCSV` primitives for maximum serialization throughput.
* **Data Sanitization & Formatting:** Built-in handling of common Elixir data types:
  - `nil` formatted as empty string `""`
  - Dates, Times, NaiveDateTimes, and DateTimes formatted using standard ISO 8601 strings
  - Decimals, Integers, and Floats serialized efficiently
  - Strings escaped according to RFC 4180
* **Dialect Customization:** Support for configurable column separators (e.g., `,`, `;`, `\t`) and line endings (`\r\n` or `\n`).

### 2.3 Ecto Database Integration
* **`Repo.stream/2` Management:** Automatically wraps cursor consumption within an explicit, safely configured database transaction.
* **Configurable Batching:** Support for custom `max_rows` (default: 500) and query timeouts.
* **Connection Release Guarantee:** Ensures the database transaction commits/rolls back immediately upon stream completion or error.
* **Preload Guard:** Validates queries and provides helpful error feedback if associations are incorrectly preloaded on streamed queries.

### 2.4 Destinations
1. **File Destination (`:file`):**
   - Stream directly into a target file path with buffered I/O.
   - Return metadata upon completion (`{:ok, %{rows: n, bytes: b}}`).
2. **Staged HTTP File Download (`:plug_staged` / `BeamExport.Plug.send_staged_export/3`):**
   - Stream query to an ephemeral temporary file at full local DB speed.
   - Release the database connection immediately.
   - Serve the file to the client using `Plug.Conn.send_file/5` or chunked download.
   - Automatically delete the temporary file after download completion or client disconnect.
3. **Direct HTTP Chunked Download (`:plug_direct` / `BeamExport.Plug.stream_chunked/3`):**
   - Direct database-to-HTTP chunked transfer for fast internal networks.
   - Safe client disconnection detection using `Enum.reduce_while/3` to abort database transactions immediately on socket closure.

### 2.5 Observability & Telemetry
* Instrumentation using standard Erlang/Elixir `:telemetry`:
  - `[:beam_export, :export, :start]` — metadata: `%{format: :csv, destination: type}`
  - `[:beam_export, :export, :chunk]` — measurements: `%{rows: count, bytes: size}`
  - `[:beam_export, :export, :stop]` — measurements: `%{duration: duration, total_rows: count, total_bytes: size}`
  - `[:beam_export, :export, :exception]` — measurements: `%{duration: duration}`, metadata: `%{error: reason}`
* Zero sensitive data leakage in telemetry payloads.

---

## 3. Out of Scope (Deferred to Future Roadmap)

The following capabilities are explicitly deferred to maintain a lean, hardened core:

* ❌ **XLSX / Excel Generation:** Microsoft OpenXML format requires complex multi-part zip packaging. It will be added in a future phase once the core engine is battle-tested.
* ❌ **S3 / GCS / Cloud Object Storage:** Cloud uploads can be achieved by streaming to a file and passing the file to existing cloud SDKs (e.g. `ExAws.S3`).
* ❌ **Oban Worker Wrappers:** Providing pre-packaged Oban job modules adds an unnecessary dependency to the core library. Users can invoke `BeamExport.to_file/3` inside their own Oban workers.
* ❌ **Phoenix LiveView UI Components:** LiveView progress widgets belong in a companion UI library.
* ❌ **Archive / Compression (Zip, Gzip):** Can be composed via `Stream` transformations if needed, but not part of core MVP.
* ❌ **Web Dashboards & Management UIs:** Not suitable for a fundamental utility library.

---

## 4. MVP Success Criteria

| Milestone Metric | Target |
|---|---|
| **Memory Footprint** | Constant memory usage (< 30 MB BEAM process heap) regardless of dataset size (tested up to 500,000 rows). |
| **Throughput** | Exceed 50,000 rows/second serialized to CSV on standard hardware. |
| **Database Pool Protection** | Staged exports hold database connections for only the query execution time, not the HTTP download time. |
| **Test Coverage** | Comprehensive automated test suite covering unit tests, RFC 4180 edge cases, database failures, and client disconnects. |
| **Zero Compiler Warnings** | Clean compilation under Elixir 1.14 through 1.17. |
