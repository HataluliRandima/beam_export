# Changelog

All notable changes to `beam_export` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-01

### Added
- **Core Streaming Engine (`BeamExport.to_stream/2`):**
  - Lazy Elixir `Stream` construction emitting nested `iodata` chunks.
  - Automatic RFC 4180 CSV serialization with quoting, CRLF/LF line endings, and custom delimiters.
  - Type formatting for primitives, decimals, dates, times, datetimes, and atoms.
  - Security formula sanitization (`sanitize_formulas: true`) against Excel/Calc formula injection.
  - Automatic map key extraction when `:headers` is specified.
- **Ecto Database Integration (`BeamExport.Ecto`):**
  - Supervised cursor streaming (`Repo.stream/2`) with managed transactions.
  - Automatic preload validation raising `BeamExport.PreloadError` upfront.
  - Configurable batching via `:max_rows` (default: 500) and transaction timeouts.
- **File Destination (`BeamExport.to_file/3`):**
  - Buffered file writing using `:delayed_write` for maximum disk throughput.
  - Atomic write option (`atomic: true`) with automated temporary file cleanup on failure.
  - Metadata reporting row counts, byte counts, and duration.
- **Plug & Phoenix Web Integration (`BeamExport.Plug`):**
  - Staged HTTP file download (`send_staged_export/4`) releasing database connections in seconds to protect pools against slow clients.
  - Ephemeral process cleanup monitor guaranteeing temporary file removal on request completion or client disconnect.
  - Direct HTTP chunking (`send_chunked_export/4`) with socket disconnect detection (`{:error, :closed}`).
- **Observability (`BeamExport.Telemetry`):**
  - Standard `:telemetry` events: `[:beam_export, :export, :start]`, `:chunk`, `:stop`, and `:exception`.
  - PII security scrubber redacting row payloads on mapping exceptions.
- **Documentation & Research:**
  - `docs/research.md` (ecosystem and market gap research).
  - `docs/adr/001-streaming-strategy.md` (architectural analysis of database streaming vs slow clients).
  - `docs/mvp.md` (scope definition).
  - `docs/api-design.md` (public API contract).
  - `docs/implementation-plan.md` (TDD milestone breakdown).
  - `docs/benchmarks.md` (memory and throughput profiling across 10k–100k rows).
  - `docs/final-review.md` (Hex package maintainer audit).
