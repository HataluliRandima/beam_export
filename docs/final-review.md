# Hex Maintainer Code & Architecture Review

**Date:** November 2026  
**Reviewer:** Principal Hex Package Maintainer  
**Package:** `beam_export` (`v0.1.0`)  
**Verdict:** Approved for Publication (with documented operational notes)  

---

## 1. Public API Evaluation

### Strengths
- **Cohesive Two-Tier Design:** The API strikes an exceptional balance between low-level streaming composability (`BeamExport.to_stream/2`) and turnkey production sinks (`to_file/3`, `Plug.send_staged_export/4`). Developers are not forced into complex builder abstractions when a simple function call suffices.
- **Idiomatic Elixir Ergonomics:** Accepting `{Repo, query}` as a source directly aligns with standard Ecto idioms. Automatic extraction of map keys matching headers when `:mapper` is omitted is a thoughtful developer experience touch.
- **Strict Return Contracts:** Uniform use of `{:ok, meta}` and `{:error, reason}` across all sinks ensures clean integration with `with` blocks in production codebases.

### Areas for Future Polish
- The options keyword list across `to_file`, `to_stream`, and `Plug` helpers is flexible, but could benefit from a structured `NimbleOptions` schema in v0.2.0 to provide compile-time or start-time validation with descriptive error messages when unrecognized keys are passed.

---

## 2. Architecture & Abstractions

### Strengths
- **No Premature Architecture Astronautics:** The library completely avoids defining arbitrary `Behaviour` modules for theoretical future formats before they are needed. Modules have single, clearly defined responsibilities:
  - `Encoder` & `Formatter`: Pure CSV serialization and type coercion.
  - `Stream`: Stream assembly and chunk batching.
  - `Ecto.Streamer`: Process-isolated cursor management.
  - `Destination.File`: Buffered disk I/O and atomic writing.
  - `Plug`: Web request lifecycle, HTTP headers, and cleanup monitors.
  - `Telemetry`: Observability emission and PII scrubbing.
- **Isolated Component Testing:** Every module is independently testable without requiring an active database or web server, as demonstrated by the test suite architecture.

### Decoupling
- Ecto and Plug dependencies are strictly optional. Applications that only require memory-bounded CSV streaming from Enumerable sources or external APIs can use `beam_export` without compiling Ecto or Plug.

---

## 3. Performance & Memory Analysis

### Strengths
- **Zero Heap Concatenation Overhead:** CSV encoding constructs nested `iodata` lists (`[first | [",", rest]]`) rather than allocating new binaries via string concatenation (`<>`). This avoids copying binary data across the BEAM heap.
- **Empirically Proven $O(1)$ Memory Footprint:** Benchmarks verified that while traditional `Repo.all` expands to 53 MB for 100k records, `BeamExport` remains flat at ~1.06 MB (50.5x memory reduction).
- **Delayed Write Disk Buffering:** File writes use `:delayed_write` to minimize Erlang port communication context switches.

### Bottleneck Considerations
- Line counting in `count_lines_in_file/1` executes a secondary streaming pass over the file to report exact row counts when the source is not a countable collection. While this pass is fast (typically < 15ms for 100k lines), future versions could maintain a running row accumulator inside the primary encoding stream to avoid the second pass entirely.

---

## 4. Reliability, Error Handling & Teardown

### Strengths
- **Guaranteed Database Connection Release:** Wrapping `Repo.stream/2` in a supervised worker within `Stream.resource/3` guarantees that when stream enumeration halts, fails, or completes, the database cursor and transaction are closed promptly.
- **Process Cleanup Monitor in Plug:** Using an ephemeral monitor process (`spawn(fn -> Process.monitor(caller) ... end)`) for staged exports ensures temporary files are deleted whether the web request succeeds, times out, crashes, or the client closes the connection.
- **Atomic File Writing:** Support for `atomic: true` prevents downstream consumers from reading partially exported files during crashes.
- **Observability Security:** Exception telemetry sanitizes `BeamExport.MappingError` by stripping record payloads, guaranteeing that PII, passwords, or customer data never leak into APM monitoring systems.

---

## 5. Known Limitations & Production Risks

1. **Ecto Preload Incompatibility:**
   - *Limitation:* Ecto queries with preloaded associations cannot be streamed.
   - *Mitigation:* `BeamExport.Ecto.Streamer.validate_query!/1` catches this upfront and raises `BeamExport.PreloadError` with clear instructions to use SQL joins or explicit field selections.
2. **Direct HTTP Streaming Danger:**
   - *Risk:* If developers bypass `send_staged_export/4` and use `send_chunked_export/4` with a database query on public networks, slow mobile clients will hold database connections open for minutes.
   - *Mitigation:* ADR 001 and the README prominently warn against direct database-to-client streaming, advocating staged temp-file exports as the production standard.
3. **Ephemeral Disk Capacity:**
   - *Risk:* Generating multi-gigabyte exports via staged export requires sufficient temporary disk space on `/tmp`.
   - *Mitigation:* Ensure hosts or container pods configure adequate volume mounts or cleanups if generating extreme file sizes.

---

## 6. Recommended Next Features (Post-MVP Roadmap)

1. **`NimbleOptions` Schema Validation:** Formally schema-validate keyword options passed to public API functions.
2. **Streaming Compression (`.csv.gz`):** Support on-the-fly gzip compression via `:zlib` stream integration.
3. **JSON Lines (`.jsonl`) and TSV Dialects:** Add first-class support for streaming JSONL and tab-separated exports.
4. **Oban Recipe Documentation:** Provide copy-pasteable architectural recipes for background export workers using Oban and S3 multipart upload.

---

## 7. Conclusion

`beam_export` represents a high-caliber addition to the Elixir open-source ecosystem. It solves a real, pervasive production failure mode with technical rigor, idiomatic design, and empirical verification. It is recommended for immediate Hex release under version `0.1.0`.
