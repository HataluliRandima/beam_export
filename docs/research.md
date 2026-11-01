# Ecosystem Research: Streaming Data Export in Elixir

**Status:** Completed  
**Author:** Principal Elixir Engineering Team  
**Date:** November 2026  

---

## Executive Summary

Data export is one of the most deceptively simple requirements in web application engineering. In the Elixir ecosystem, naive implementations using `Repo.all/2` combined with in-memory CSV serialization frequently lead to severe memory spikes, BEAM garbage collection thrashing, and out-of-memory (OOM) crashes in production.

While the Elixir standard library and community packages provide low-level primitives (`Stream`, `Ecto.Repo.stream/2`, `NimbleCSV`, `Plug.Conn.chunk/2`), there is **no unified, production-hardened streaming export engine**. Developers are forced to repeatedly hand-roll 50–100 lines of brittle boilerplate to stitch database transactions, stream transformations, CSV chunking, HTTP client backpressure, and resource cleanup together.

More dangerously, common community recipes advocate piping an `Ecto.Repo.stream/2` directly to `Plug.Conn.chunk/2`. In production, this ties a PostgreSQL database connection and active transaction to a potentially slow or malicious HTTP client, causing **connection pool starvation** and **database transaction bloat**.

This document investigates the ecosystem, identifies the market gap, analyzes package naming, and presents a definitive recommendation.

---

## 1. Existing Solutions

We analyzed the current tools, libraries, and patterns used by Elixir developers for large-scale data export.

### 1.1 `NimbleCSV` (`~> 1.2`)
* **What problem it solves:** Extremely fast, RFC 4180-compliant CSV parsing and serialization using binary pattern matching and compile-time dialect definitions.
* **Architecture:** Generates optimized parse and dump functions via macros (`NimbleCSV.define/2`). Exposes `dump_to_stream/1` and `dump_to_iodata/1`.
* **Strengths:**
  - Micro-optimized for speed and minimal allocations.
  - Generates `iodata` / `io_list`, avoiding binary concatenation overhead.
  - Zero external dependencies.
* **Limitations:**
  - Pure encoding/decoding engine only.
  - Has no awareness of database cursors, batching, transactions, or backpressure.
  - Does not manage headers, field extraction from structs/maps, or destination IO.
* **Overlap:** High synergy. `NimbleCSV` is the ideal underlying CSV serialization primitive for our engine rather than a competitor.

### 1.2 `CSV` (`beatrichartz/csv`, `~> 3.2`)
* **What problem it solves:** Full-featured CSV encoding and decoding with native stream interfaces.
* **Architecture:** Relies on Elixir protocols and `Stream` transformations (`CSV.encode/2`).
* **Strengths:**
  - Easy to pipe directly with `Stream.map/2`.
  - Configurable headers and escaping.
* **Limitations:**
  - Significantly slower than `NimbleCSV` (benchmark tests often show 3x–6x lower throughput).
  - Emits binaries per row rather than leveraging nested iodata efficiently across chunks.
  - Does not handle database transaction lifecycles or safe staging.
* **Overlap:** Alternative encoding engine, but less suited for high-throughput streaming than `NimbleCSV`.

### 1.3 `Ecto.Repo.stream/2` (`ecto_sql`)
* **What problem it solves:** Lazy database query execution using database-native cursors (via `postgrex` or `myxql`).
* **Architecture:** Uses PostgreSQL `DECLARE CURSOR` / `FETCH` under the hood. Batches rows lazily (default `max_rows: 500`).
* **Strengths:**
  - Memory usage inside the BEAM remains bounded to the batch size.
  - Native integration with Ecto query syntax.
* **Limitations:**
  - **Mandatory Transaction:** Must run inside an explicit `Repo.transaction/2` block.
  - **Cannot Preload Associations:** Attempting `preload` on streamed queries raises runtime errors or exhibits undefined behavior.
  - **Connection Hogging:** Holds a dedicated database pool connection for the entire lifetime of the stream enumeration.
  - **Transaction Locks:** Long-running streams hold open database snapshots, preventing PostgreSQL `VACUUM` from cleaning up dead tuples (catalog/table bloat).
* **Overlap:** The core data source for relational datasets in our library.

### 1.4 Plug & Phoenix HTTP Chunking (`Plug.Conn.chunk/2`, `send_chunked/2`)
* **What problem it solves:** Sending chunked HTTP responses (Transfer-Encoding: chunked) over Cowboy or Bandit without buffering the entire payload in memory.
* **Architecture:** Iteratively pushes byte chunks to the underlying web server socket. `Plug.Conn` also implements the `Collectable` protocol.
* **Strengths:**
  - Low time-to-first-byte (TTFB).
  - Memory consumption on the server is independent of download size.
* **Limitations:**
  - **Zero Database Protection:** When directly piped from `Repo.stream`, a client downloading at 50 KB/s on a 200 MB export will hold a database connection open for over an hour.
  - **Socket Teardown:** If a client abruptly closes the connection, the writer process only discovers the failure on the next attempted write (`{:error, :closed}`). If error handling is not explicit (`Enum.reduce_while/3`), the BEAM process can crash or leak uncollected transaction state.
* **Overlap:** One of the export destinations, but must be paired with safety mechanisms.

### 1.5 Phoenix `send_download/3`
* **What problem it solves:** Sending file downloads to HTTP clients.
* **Architecture:** Supports `:file` paths (utilizing kernel `sendfile` syscall where available) or in-memory `:binary` payloads.
* **Strengths:**
  - `:file` downloads with `sendfile` are exceptionally efficient, bypass BEAM heap allocations, and do not hold database connections.
* **Limitations:**
  - Requires the file to already exist on disk.
  - If used with `:binary`, it re-introduces the exact memory bloat problem we are solving.
* **Overlap:** Validates the two-stage export architecture: Stream to temp file -> serve via file download.

### 1.6 Excel / XLSX Generation (`elixlsx`, `xlsxir`)
* **What problem it solves:** Generating spreadsheet documents.
* **Architecture:** XLSX is an OpenXML standard: a zip archive containing multiple interdependent XML files (worksheets, styles, shared strings, workbook metadata).
* **Limitations:**
  - True streaming of XLSX over HTTP is notoriously difficult because zip central directories and shared string tables require either multi-pass generation or buffering tables.
* **Overlap:** Out of scope for MVP; informs our architectural decoupling so future formats (TSV, JSONL, XLSX) can plug into the pipeline.

### 1.7 Background Workers (`Oban`, `Oban.Pro`)
* **What problem it solves:** Asynchronous, reliable background job processing with retries and concurrency control.
* **Architecture:** Database-backed queue.
* **Strengths:**
  - Isolates heavy database extraction and file generation from web request cycles.
* **Limitations:**
  - Requires job queues, storage backend (S3/GCS/local disk), and notification mechanisms (email, WebSockets/LiveView).
* **Overlap:** Future integration target; the core export engine should be runnable inside an Oban job without modifying engine internals.

---

## 2. Market Gap Analysis

### What do developers do today?
Currently, every engineering team implements ad-hoc code:
```elixir
def export_users(conn) do
  conn = conn |> put_resp_content_type("text/csv") |> send_chunked(200)

  Repo.transaction(fn ->
    User
    |> Repo.stream(max_rows: 1000)
    |> Stream.map(&format_user/1)
    |> NimbleCSV.dump_to_stream()
    |> Enum.reduce_while(conn, fn chunk, conn ->
      case Plug.Conn.chunk(conn, chunk) do
        {:ok, conn} -> {:cont, conn}
        {:error, :closed} -> {:halt, conn}
      end
    end)
  end, timeout: :infinity)
end
```

### What is fundamentally missing?
1. **No Safeguards Against Connection Pool Exhaustion:** Tutorials ubiquitously recommend `timeout: :infinity` on `Repo.transaction`. In high-traffic or multi-tenant production systems, 5 concurrent slow downloads will exhaust a default pool of 10 connections, bringing the entire API down.
2. **Missing "Staged Streaming" Abstraction:** The safest pattern for large exports is to stream from the database into a temporary file at maximum local DB throughput (typically taking 1–3 seconds), release the DB connection immediately, and then stream the file to the client or upload it to storage. No package provides this turnkey workflow.
3. **Boilerplate & Error Handling Burden:** Developers must manually handle CSV header prepending, nil-safe column mapping, encoding transformations, temporary file lifecycles with guaranteed deletion (`try/after`), and database rollback semantics on client disconnect.
4. **Lack of Operational Telemetry:** Real-world systems need metrics: records exported, bytes generated, time spent in database cursor vs time spent writing to destination, and chunk-level throughput.

---

## 3. Naming Research

We evaluated candidate package names against Hex.pm, GitHub, and Elixir naming conventions:

| Rank | Name | Module | Hex.pm Status | Clarity & Memorability | Evaluation |
|---|---|---|---|---|---|
| **1** | `beam_export` | `BeamExport` | **Available** | High | Clean, idiomatic, directly captures robust data export on the BEAM. Format-agnostic for future expansion. |
| **2** | `stream_export` | `StreamExport` | Available (used internally by some packages, but top-level package is free) | Medium | Descriptive, but slightly generic; less distinct brand identity. |
| **3** | `ecto_export` | `EctoExport` | **Available** | Medium | Overly restrictive name: implies coupling solely to Ecto, whereas streams can originate from files, APIs, or memory. |

**Selected Name:** `beam_export` (`BeamExport`).

---

## 4. Final Recommendation: GO (with Strategic Architectural Pivot)

**Decision: GO**

### Rationale:
There is a clear, persistent pain point in the Elixir ecosystem: handling large exports reliably without either crashing BEAM nodes or starving database pools.

However, we **MUST NOT** build a library whose primary recommendation is piping an `Ecto.Repo.stream` directly to a slow HTTP client. Doing so would encourage an anti-pattern under the guise of "idiomatic streaming".

### The Strategic Pivot:
`beam_export` will be built on three core pillars:
1. **Predictable Memory (Streaming Pipeline):** Pure, lazy streaming from data source to encoding to sink using `iodata` and `NimbleCSV`.
2. **Database Protection (Safe Staging by Default):**
   - Provide **Staged File Streaming** (`Source -> Temp File -> Destination`) as the recommended path for HTTP downloads, detaching the DB connection in seconds.
   - Provide **Direct File Export** for background jobs, local storage, and cloud uploads.
   - Provide **Direct HTTP Streaming** as an explicit option with documented trade-offs, configurable timeouts, and graceful client-disconnect teardown.
3. **Comprehensive Observability:** First-class `:telemetry` instrumentation out of the box to track rows, bytes, DB duration, and total transfer time.

This foundation ensures `beam_export` is not just another utility wrapper, but a production-grade library that senior engineers can confidently adopt in high-load systems.

