# Implementation Plan: `beam_export`

**Status:** Approved  
**Author:** Principal Elixir Engineering Team  
**Date:** November 2026  
**Project:** `beam_export`  

---

## Overview

This implementation plan breaks the delivery of `beam_export` into seven independently verifiable milestones. Each milestone is designed following test-driven development (TDD), with explicit goals, module scopes, automated test requirements, acceptance criteria, and identified risks.

```text
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│  MILESTONE 1    │ ----> │  MILESTONE 2    │ ----> │  MILESTONE 3    │
│  CSV Formatting │       │  Core Stream &  │       │  Ecto Query     │
│  & Escaping     │       │  Pipeline       │       │  Streaming      │
└─────────────────┘       └─────────────────┘       └─────────────────┘
                                                             │
                                                             ▼
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│  MILESTONE 6    │ <---- │  MILESTONE 5    │ <---- │  MILESTONE 4    │
│  Telemetry &    │       │  Plug / HTTP    │       │  File Sink &    │
│  Observability  │       │  Integrations   │       │  Buffer Writing │
└─────────────────┘       └─────────────────┘       └─────────────────┘
         │
         ▼
┌─────────────────┐
│  MILESTONE 7    │
│  Benchmarks &   │
│  Verification   │
└─────────────────┘
```

---

## Milestone 1: CSV Encoding & Value Formatting Engine

* **Goal:** Implement a fast, memory-efficient RFC 4180 CSV serialization engine using `iodata` and standard Elixir data type formatting.
* **Modules:**
  - `BeamExport.CSV.Encoder`
  - `BeamExport.CSV.Formatter`
* **Tests:**
  - `test/beam_export/csv/encoder_test.exs`
  - Quoting rules: commas, double quotes, CRLF, LF, unicode characters.
  - Type formatting: `nil` to `""`, dates, times, decimals, integers, floats, booleans.
  - Security: Formula sanitization (`=`, `+`, `-`, `@`) when enabled.
* **Acceptance Criteria:**
  - Generates valid RFC 4180 iodata chunks.
  - Zero regex allocations on clean fields.
  - Passes 100% of formatting edge cases.
* **Risks:** Slow escaping logic. Mitigated by using Erlang binary pattern matching and iodata nesting.

---

## Milestone 2: Core Stream Abstraction & Pipeline

* **Goal:** Build the lazy streaming pipeline that accepts any `Enumerable`, handles row mapping, prepends headers, chunks rows, and streams encoded iodata.
* **Modules:**
  - `BeamExport` (main facade)
  - `BeamExport.Stream`
* **Tests:**
  - `test/beam_export/stream_test.exs`
  - Empty enumerables.
  - Stream with headers only.
  - Custom mapper transformations.
  - Large simulated collections (100,000 items) verifying constant memory.
  - Mapper exceptions and graceful termination.
* **Acceptance Criteria:**
  - `BeamExport.to_stream/2` returns a lazy `Enumerable.t()`.
  - Lazy evaluation is strictly maintained until a terminal sink is executed.
* **Risks:** Memory leaks through retained closures. Mitigated by keeping mapper functions pure.

---

## Milestone 3: Ecto Database Integration

* **Goal:** Safely encapsulate `Ecto.Repo.stream/2` within managed transactions with configurable batch size and preload validation.
* **Modules:**
  - `BeamExport.Ecto`
* **Tests:**
  - `test/beam_export/ecto_test.exs`
  - Streaming from in-memory / SQLite / mock Ecto adapter.
  - Transaction lifecycle: verifying transaction commits after stream completion.
  - Query failure handling and transaction rollback.
  - Query preload detection and descriptive error raising.
* **Acceptance Criteria:**
  - `BeamExport.to_stream({Repo, query})` manages transaction opening and closing deterministically via `Stream.resource/3`.
  - Batching matches `:max_rows` (default 500).
* **Risks:** Hanging transactions on early stream halt. Mitigated by testing early halting with `Stream.take/2`.

---

## Milestone 4: File Destination Sink

* **Goal:** Provide `BeamExport.to_file/3` for writing streams directly to disk with buffered I/O, error handling, and file statistics.
* **Modules:**
  - `BeamExport.Destination.File`
* **Tests:**
  - `test/beam_export/destination/file_test.exs`
  - Writing small, medium, and large streams to temporary files.
  - Verification of byte count and row count metrics.
  - Error handling: Non-existent directories, permission errors, disk full simulation.
* **Acceptance Criteria:**
  - Returns `{:ok, %{rows: count, bytes: size, path: path}}` or `{:error, reason}`.
  - Guaranteed file descriptor closure.
* **Risks:** Partial file writes on failure. Mitigated by atomic write option or explicit cleanup on error.

---

## Milestone 5: Plug & Phoenix Web Integration

* **Goal:** Implement the staged temporary file HTTP export (recommended) and direct chunked HTTP export with client disconnect detection.
* **Modules:**
  - `BeamExport.Plug`
* **Tests:**
  - `test/beam_export/plug_test.exs`
  - Staged export: Verifies headers (`Content-Type`, `Content-Disposition`), file streaming, and temp file deletion.
  - Direct chunked export: Verifies chunked transfer encoding and headers.
  - Disconnect handling: Verifies `{:error, :closed}` halts the stream and aborts transaction without crashing.
* **Acceptance Criteria:**
  - Works with standard `Plug.Conn` and `Plug.Test`.
  - Temporary files are guaranteed to be cleaned up after request finishes.
* **Risks:** Plug dependency version incompatibility. Mitigated by marking `plug` as optional in `mix.exs`.

---

## Milestone 6: Telemetry & Observability

* **Goal:** Instrument all export phases with standardized `:telemetry` events.
* **Modules:**
  - `BeamExport.Telemetry`
* **Tests:**
  - `test/beam_export/telemetry_test.exs`
  - Verifying `[:beam_export, :export, :start]`, `[:beam_export, :export, :chunk]`, and `[:beam_export, :export, :stop]` events.
  - Verifying error events on failure `[:beam_export, :export, :exception]`.
  - Ensuring no sensitive row values leak into telemetry metadata.
* **Acceptance Criteria:**
  - Emits accurate monotonic durations, row counts, and byte measurements.
* **Risks:** Overhead of frequent telemetry emissions. Mitigated by emitting chunk-level telemetry per batch rather than per row.

---

## Milestone 7: Benchmarks & Production Hardening

* **Goal:** Validate performance claims with reproducible benchmarks comparing `Repo.all` + `Enum` vs `BeamExport.to_file` and `BeamExport.to_stream`.
* **Modules:**
  - `bench/export_bench.exs`
* **Tests & Verification:**
  - Measure memory consumption (BEAM heap peak) across 10k, 50k, 100k, and 250k rows.
  - Measure execution speed and throughput (rows/sec).
  - Document benchmark results in `docs/benchmarks.md`.
* **Acceptance Criteria:**
  - Memory consumption of `BeamExport` remains flat (< 30 MB), whereas traditional `Repo.all` grows linearly with row count.
* **Risks:** Benchmarking noise in containerized/virtualized environments. Mitigated by averaging multiple runs and reporting median values.
