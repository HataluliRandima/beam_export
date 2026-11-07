# Changelog

All notable changes to `beam_export` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

First public release.

### Added

- **Exports** from any `Enumerable` or `{Repo, queryable}` Ecto source:
  - `BeamExport.to_file/3`: atomic file writes (temporary file, `fsync`, rename),
    optional streaming gzip (`compress: true` or a `.gz` path), files created with
    mode `0600` (`:file_mode`).
  - `BeamExport.to_iodata/2`: in-memory exports for small datasets.
  - `BeamExport.to_stream/2`: lazy stream of CSV `iodata` chunks.
- **Plug/Phoenix downloads** (`BeamExport.Plug`, requires `:plug`):
  - `send_staged_export/4` writes the export to a private (`0700`) temporary
    directory, releases the database connection, then serves the file with
    `send_file`. Temporary files are removed when the request process exits.
  - `send_chunked_export/4` streams with chunked transfer encoding and stops reading
    the source when the client disconnects.
- **CSV encoding**: RFC 4180 quoting, custom separators, `"\r\n"` or `"\n"` line
  terminators. Exact floats (`Float.to_string/1`), decimals in plain notation,
  ISO 8601 dates and times, tuple rows (Ecto `select: {...}`), and map/struct records
  read by `:headers`.
- **Formula injection protection**, enabled by default (`sanitize_formulas: true`),
  following the OWASP recommendation.
- **Ecto integration**: cursor streaming with `:max_rows` batches; eager rejection of
  preloads (`BeamExport.PreloadError`); reuses the caller's transaction when there is
  one; uses `Repo.transact/2` on Ecto 3.13+ and `Repo.transaction/2` before.
- **Errors**: `BeamExport.MappingError`, `BeamExport.EncodeError`,
  `BeamExport.TimeoutError`, `BeamExport.ExportError`, `BeamExport.PreloadError`.
  Record contents are redacted by default (`:include_record_in_errors` opts in).
- **Option validation**: unknown options and invalid values raise `ArgumentError`.
- **Telemetry**: `[:beam_export, :export, :start | :chunk | :stop | :exception]`
  events from `to_file/3` and `to_iodata/2`. Exception metadata is always redacted.

### Behaviour notes

These describe guarantees that a pre-release audit found missing in earlier
development snapshots:

- An export that does not complete is never reported as successful. This covers
  stalled consumers (`:consumer_timeout`), unresponsive databases (`:batch_timeout`),
  failed commits, crashed workers, and write errors reported only when a file is
  closed (for example a full disk).
- A failed export never deletes a file it did not create or truncate, and never
  deletes non-regular files.
- `meta.rows` counts the header row.
