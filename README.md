# BeamExport

[![Hex.pm](https://img.shields.io/hexpm/v/beam_export.svg)](https://hex.pm/packages/beam_export)
[![HexDocs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/beam_export)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Memory-bounded CSV exports for Elixir: from **Ecto queries** or **any enumerable**
to **files**, **iodata** and **Plug/Phoenix downloads**, without holding a database
connection for as long as a slow client takes to download.

---

## The problem

Large CSV exports commonly hurt production systems in two ways.

**1. Memory spikes (`Repo.all`).** Loading every row, then building the whole CSV in
memory, makes memory grow with the size of the export:

```elixir
# Loads the entire dataset into the process heap
Repo.all(query) |> Enum.map(&format_row/1) |> CSV.encode() |> send_download(...)
```

**2. Connection pool starvation (naive HTTP streaming).** `Repo.stream/2` must run
inside a transaction, so streaming it straight to an HTTP client holds a database
connection for the entire download:

```elixir
# Holds a pool connection for as long as the slowest client takes
Repo.transaction(fn ->
  query |> Repo.stream() |> NimbleCSV.dump_to_stream() |> Enum.into(conn)
end, timeout: :infinity)
```

A handful of slow downloads can exhaust the pool. Long transactions also delay
PostgreSQL vacuum.

## The approach

BeamExport streams rows through a lazy pipeline whose memory is proportional to
`:chunk_size`, not to the export. For HTTP downloads, its recommended path stages the
export on local disk first, so the database connection is released as soon as the
rows are read, then serves the file with `sendfile`:

```text
STAGE 1  database cursor ──> CSV chunks ──> private temp file      (holds 1 DB connection)
                                                │
         ── transaction commits, connection returns to the pool ──
                                                │
STAGE 2  HTTP client  <────── send_file ─────── temp file          (no DB connection)
                                                │
         ── temp file deleted when the request process exits ──
```

What it does for you:

* **Bounded memory.** Rows are fetched (`:max_rows`), encoded and written in chunks.
* **Atomic files.** `to_file/3` writes a temporary file, syncs it and renames it
  over the target, so readers never see a partial file. Disk-full and other write
  errors, including errors only reported when the file is closed, fail the export.
* **Fails loudly.** An export that does not complete (a crash, a rolled-back
  transaction, a consumer or database that stops making progress) returns or raises
  an error. It is never reported as a successful but shorter export.
* **Safe defaults.** Spreadsheet formula injection protection is on; exported files
  are created `0600`; errors and telemetry never contain row data.
* **Correct CSV.** RFC 4180 quoting; exact floats and decimals; values without a
  faithful text form (lists, maps, tuples) are rejected instead of being mangled.
* **Observability.** `:telemetry` events for start, chunks, stop and exceptions.

---

## Installation

```elixir
def deps do
  [
    {:beam_export, "~> 0.1.0"},

    # Optional integrations:
    {:ecto, "~> 3.10"},      # Ecto sources (usually via ecto_sql and an adapter)
    {:plug, "~> 1.14"},      # BeamExport.Plug (Phoenix already includes Plug)
    {:decimal, "~> 2.0"}     # Decimal values (already included with Ecto)
  ]
end
```

Requires Elixir 1.15 or later.

---

## Quick start

### Export an Ecto query to a file (background jobs, CLI tasks)

```elixir
import Ecto.Query

query = from(u in MyApp.Accounts.User, where: u.active, order_by: u.id)

{:ok, meta} =
  BeamExport.to_file({MyApp.Repo, query}, "/var/app/exports/active_users.csv",
    headers: ["ID", "Full Name", "Email Address", "Created At"],
    mapper: fn user -> [user.id, user.name, user.email, user.inserted_at] end
  )

# meta => %{path: "...", rows: 50_001, bytes: 2_450_120, compressed: false, duration: ...}
# `rows` includes the header row; `duration` is in :native time units.
```

Write exports somewhere private. Never under `priv/static`, which is publicly served.

### Phoenix download (staged, recommended)

```elixir
defmodule MyAppWeb.UserExportController do
  use MyAppWeb, :controller
  import Ecto.Query

  def index(conn, _params) do
    query = from(u in MyApp.Accounts.User, order_by: u.id)

    BeamExport.Plug.send_staged_export(conn, "users.csv", {MyApp.Repo, query},
      headers: ["User ID", "Name", "Email"],
      mapper: fn u -> [u.id, u.name, u.email] end
    )
  end
end
```

The export is written to a private temporary directory, the transaction commits, and
the file is sent with `sendfile`. The temporary files are removed when the request
process exits. If the export fails, `BeamExport.ExportError` is raised before any
response is sent (a 500 in Phoenix).

### Without a mapper

Lists and tuples are used as rows directly. Maps and structs are read by `:headers`,
which must then be their keys:

```elixir
# Ecto `select` returning lists or tuples:
BeamExport.to_file({Repo, from(u in User, select: {u.id, u.email})}, path, headers: ["ID", "Email"])

# Maps or structs, read by key:
BeamExport.to_iodata([%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}], headers: [:id, :name])
```

Use a `:mapper` when you want human-readable titles for struct fields. Headers such as
`"Email Address"` do not match any struct field, so BeamExport raises
`BeamExport.EncodeError` instead of exporting blank columns.

### Lazy streams

```elixir
BeamExport.to_stream(rows, headers: ["ID", "Name"])
|> Stream.into(File.stream!("users.csv"))
|> Stream.run()
```

For an Ecto source outside a transaction, `to_stream/2` runs the cursor in a worker
process that holds the connection until the stream finishes. Consume the stream
promptly: if the consumer stops asking for data for `:consumer_timeout` (default 30 s),
the cursor is closed and `BeamExport.TimeoutError` is raised.

### Gzip

```elixir
BeamExport.to_file({MyApp.Repo, query}, "/var/app/exports/users.csv.gz", mapper: &[&1.id, &1.email])
```

A `.gz` path (or `compress: true`) streams through gzip. Staged Plug downloads of
compressed files are sent as `application/gzip`.

---

## Options

Unknown options raise `ArgumentError`, so a typo cannot silently turn a feature off.

| Option | Applies to | Default | Description |
|---|---|---|---|
| `:headers` | all | none | Column titles (strings/atoms) written first. Also the keys read from map/struct records when there is no `:mapper`. |
| `:mapper` | all | none | `fn record -> [field, ...] end` (a tuple is accepted too). |
| `:chunk_size` | all | `500` | Rows per chunk / file write. |
| `:separator` | all | `","` | Field separator. Must not contain `"`, CR or LF. |
| `:line_terminator` | all | `"\r\n"` | `"\r\n"` or `"\n"`. |
| `:sanitize_formulas` | all | `true` | Prefix text starting with `= + - @` TAB or CR with `'` (see Security). |
| `:include_record_in_errors` | all | `false` | Put record contents in `MappingError` (never in telemetry). |
| `:max_rows` | Ecto | `500` | Rows per database fetch. |
| `:timeout` | Ecto | `:infinity` | Maximum time the export may hold its DB connection; the export fails when it elapses. |
| `:consumer_timeout` | `to_stream`, chunked | `30_000` | Max wait for the consumer to request the next batch. |
| `:batch_timeout` | `to_stream`, chunked | `120_000` | Max wait for the database to deliver the next batch. |
| `:atomic` | `to_file`, staged | `true` | Temp file + sync + rename. With `false`, a failed export still removes its partial file, but the previous contents of the target are lost. |
| `:compress` | `to_file`, staged | by extension | `true`/`:gzip` or `false`; defaults to on for `.gz` paths. |
| `:file_mode` | `to_file`, staged | `0o600` | Permissions of created files. |
| `:tmp_dir` | staged | `System.tmp_dir!()` | Parent of the private temporary directory. |
| `:content_type`, `:charset` | Plug | `"text/csv"`, `"utf-8"` | Response content type. |

---

## Values

| Value | CSV text |
|---|---|
| `nil` | empty field |
| strings, atoms | as-is (formula-sanitized by default) |
| integers | `42` |
| floats | shortest exact form: `0.1`, `1.0e-12` |
| `Decimal` | plain notation, never scientific: `1000`, `-0.00012` |
| `Date` / `Time` / `NaiveDateTime` / `DateTime` | ISO 8601 |
| booleans | `true` / `false` |
| structs implementing `String.Chars` (e.g. `URI`) | `to_string/1` |
| lists, maps, tuples, other terms | `BeamExport.EncodeError`: convert them in your `:mapper` |

---

## Errors

`to_file/3` and `to_iodata/2` return `{:ok, ...}` or `{:error, exception}`.
`to_stream/2` raises the same exceptions while it is consumed. Invalid options raise
`ArgumentError`.

| Exception | Meaning |
|---|---|
| `BeamExport.MappingError` | The mapper raised, or returned something other than a list/tuple. Has `:row` (1-based) and `:reason`. |
| `BeamExport.EncodeError` | A record or value has no faithful CSV form, or headers do not match the records. Has `:row`, `:column`, `:reason`. |
| `BeamExport.PreloadError` | The query has preloads (not supported by database cursors). |
| `BeamExport.TimeoutError` | An Ecto stream stopped making progress (`:consumer_timeout` / `:batch_timeout`). The export is incomplete. |
| `BeamExport.ExportError` | Other failures (for example a transaction that did not commit, or a failed staged download). |
| `File.Error`, `File.RenameError` | Filesystem errors, including a full disk. |

```elixir
case BeamExport.to_file(source, path, opts) do
  {:ok, %{rows: rows}} -> {:ok, rows}
  {:error, %BeamExport.MappingError{row: row}} -> {:error, "bad record at row #{row}"}
  {:error, exception} -> {:error, Exception.message(exception)}
end
```

---

## Security

* **CSV / formula injection.** By default, text fields starting with `=`, `+`, `-`,
  `@`, TAB or CR (also after leading whitespace) get a leading `'`, so Excel,
  LibreOffice and Google Sheets display them instead of evaluating them (OWASP
  recommendation). This intentionally changes such text: a *string* `"-5"` becomes
  `"'-5"` (numbers are never changed). For files consumed by programs rather than
  spreadsheets, pass `sanitize_formulas: false`.
* **No row data in errors or telemetry.** `MappingError`s are redacted: they report
  the row number and a safe description (for example `key :email not found`), not the
  record. Stacktraces in telemetry carry arities, not arguments. Opt in with
  `include_record_in_errors: true` while debugging. Exceptions raised by your own
  source or by the database driver are passed through unchanged.
* **Files.** Exports are created with mode `0600`. Staged downloads use a fresh `0700`
  directory per request. Never pass user input as a file path or `:tmp_dir`.
* **Limits are your job.** BeamExport does not limit export size or concurrency.
  Every in-flight export holds one database connection while it reads rows (staged
  downloads too, until the file is written), so concurrent exports beyond your pool
  size starve the rest of the application. Cap them below `pool_size` (for example
  with an Oban queue limit or a semaphore), put authorization and rate limiting in
  front of export endpoints, and make sure the temporary directory has room for
  concurrent exports. In containers, `/tmp` may be RAM-backed.

---

## Performance

Measured on Linux (OTP 26, Elixir 1.17) writing a 5-column synthetic dataset with
`to_file/3`, single runs:

| Rows | Time | Rows/s | Peak process memory |
|---|---|---|---|
| 10,000 | 66 ms | ~150k | 0.4 MB |
| 100,000 | 477 ms | ~210k | 0.4 MB |
| 300,000 | 1.6 s | ~190k | 0.3 MB |

From SQLite, 50,000 rows took about 0.5 s with `to_file/3`. Memory is proportional to
`chunk_size × row size`, so wide rows or large text fields use more. Disk usage and
time grow linearly with the export. Your numbers will depend on the database, row
width and mapper. `bench/export_bench.exs` contains a reproducible benchmark
(`MIX_ENV=test mix run bench/export_bench.exs`).

---

## Limitations

* **Preloads** are not supported (`BeamExport.PreloadError`); use joins or `select`.
* **Staged downloads block** until the whole file is written, so very large exports
  can hit proxy or load balancer timeouts. Run those with `to_file/3` in a background
  job (for example Oban) and serve the result separately.
* **Staged temporary files are removed when the request process exits.** Some
  servers (for example Bandit with HTTP/1.1 keep-alive) reuse one process for
  several requests on a connection, so files can outlive the request until the
  connection closes. A crashed node can leave files behind in `:tmp_dir`.
* **`send_chunked_export/4`** holds a database connection for the whole download. Use
  it only for fast, trusted clients.
* **`to_stream/2` and `send_chunked_export/4` emit no telemetry.**
* **Atomic writes and power loss.** `to_file/3` syncs the file to disk before the
  rename, but not the parent directory (Erlang/OTP has no portable way to do that). On
  some filesystems, a crash or power loss right after the rename can leave the
  previous file in place.
* **`meta.rows` includes the header row.**
* CSV only for now; see [ROADMAP.md](ROADMAP.md).

---

## Telemetry

| Event | Measurements | Metadata |
|---|---|---|
| `[:beam_export, :export, :start]` | `system_time` | `destination`, `format` |
| `[:beam_export, :export, :chunk]` | `rows`, `bytes` (uncompressed) | `destination` |
| `[:beam_export, :export, :stop]` | `duration` (native units), `rows`, `bytes` (on disk) | `destination`, `format` |
| `[:beam_export, :export, :exception]` | `duration` (native units) | `destination`, `kind`, `reason`, `stacktrace`, `format` |

Emitted by `to_file/3` (`destination: :file`, also used by staged downloads) and
`to_iodata/2` (`destination: :iodata`). See `BeamExport.Telemetry`.

---

## Contributing

1. Fork the repository and create a branch.
2. `mix test` (the disk-full tests need `/dev/full`, so they run on Linux only).
3. `mix format` and `mix compile --warnings-as-errors`.
4. Open a pull request describing the change.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
