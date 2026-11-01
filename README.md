# BeamExport

[![Hex.pm](https://img.shields.io/hexpm/v/beam_export.svg)](https://hex.pm/packages/beam_export)
[![HexDocs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/beam_export)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A production-grade, memory-bounded streaming data export engine for Elixir.

Export large datasets from **Ecto queries** or **arbitrary enumerables** to **RFC 4180-compliant CSV files** and **HTTP downloads** with strictly constant $O(1)$ memory usage and built-in database connection pool protection.

---

## 💥 The Problem

In Elixir web applications, large CSV exports frequently bring down production systems in one of two ways:

### Failure Mode 1: Memory Explosion (`Repo.all`)
```elixir
# ❌ DANGEROUS: Loads the entire dataset into BEAM process heap
Repo.all(query)
|> Enum.map(&format_row/1)
|> CSV.encode()
|> send_download(...)
```
For datasets with 50,000+ rows, this causes:
- Multi-hundred megabyte memory spikes on the BEAM heap.
- Severe Garbage Collection (GC) pauses affecting concurrent processes.
- Out-of-memory (OOM) crashes killing the entire BEAM node.

### Failure Mode 2: Database Pool Starvation (Naive HTTP Streaming)
```elixir
# ❌ DANGEROUS: Ties a database connection to an external browser
Repo.transaction(fn ->
  query
  |> Repo.stream()
  |> NimbleCSV.dump_to_stream()
  |> Enum.into(conn)
end, timeout: :infinity)
```
- `Repo.stream/2` must run inside an active database transaction.
- If a mobile client or slow connection downloads a 100 MB export over 15 minutes, **that database connection is checked out for 15 minutes**.
- Just 5 to 10 concurrent export downloads will **consume 100% of your Ecto pool**, causing cascading connection timeouts across your entire application!
- Long transactions prevent PostgreSQL autovacuum from cleaning dead tuples, causing acute table bloat.

---

## 🛡️ The Solution: `beam_export`

`beam_export` provides a hardened, two-stage export architecture:

```text
STAGE 1: FAST DATABASE EXTRACTION (Runs at local DB speed in 1-2 seconds)
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│ Database Cursor │ ────> │  iodata Stream  │ ────> │ Temporary File  │
│ (Holds DB Conn) │       │  (Bounded Mem)  │       │ on Local Disk   │
└─────────────────┘       └─────────────────┘       └────────┬────────┘
                                                             │
[DATABASE TRANSACTION COMMITS & CONNECTION RETURNED TO POOL IMMEDIATELY]
                                                             │
STAGE 2: SERVE OVER HTTP WITHOUT HOLDING DATABASE RESOURCES  │
                                                             ▼
┌─────────────────┐                                 ┌─────────────────┐
│   HTTP Client   │ <────────────────────────────── │ Temporary File  │
│  (Slow/Mobile)  │     (sendfile kernel bypass)    │ on Local Disk   │
└─────────────────┘                                 └────────┬────────┘
                                                             │
[EPHEMERAL MONITOR PROCESS GUARANTEES FILE DELETION UPON COMPLETION/DISCONNECT]
```

### Why use `beam_export` instead of hand-rolling code?
* 🚀 **50x Lower Process Memory:** Benchmarks prove memory stays flat at **~1.06 MB** regardless of whether exporting 1,000 or 100,000 records.
* 🛡️ **Database Connection Safety:** Turns long web downloads into fast 1-second database dumps, releasing pool connections immediately.
* ⚡ **High Throughput:** Encodes nested `iodata` chunks directly, exceeding **130,000 rows/second** on standard hardware.
* 🧹 **Zero Leaked Resources:** Ephemeral monitor processes guarantee temporary files and database transactions are cleaned up even if the client disconnects or crashes.
* 🔒 **Security Hardened:** Built-in formula sanitization prevents CSV injection attacks (`=`, `+`, `-`, `@`) in spreadsheet applications.
* 📡 **Production Observability:** Built-in `:telemetry` events for rows, bytes, and durations with automatic PII sanitization.

---

## 📦 Installation

Add `beam_export` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:beam_export, "~> 0.1.0"},
    # Optional integrations:
    {:ecto_sql, "~> 3.10"}, # if exporting from Ecto
    {:plug, "~> 1.14"}      # if exporting via Phoenix/Plug HTTP
  ]
end
```

---

## 🚀 Quick Start

### 1. Export an Ecto Query to a File (Background Jobs, Oban, CLI)

```elixir
import Ecto.Query

query = from(u in MyApp.Accounts.User, where: u.active == true, order_by: u.id)

{:ok, meta} = BeamExport.to_file(
  {MyApp.Repo, query},
  "priv/static/exports/active_users.csv",
  headers: ["ID", "Full Name", "Email Address", "Created At"],
  mapper: fn user ->
    [user.id, user.name, user.email, user.inserted_at]
  end,
  max_rows: 1_000,
  atomic: true
)

# meta => %{path: "...", rows: 50001, bytes: 2450120, duration: 420000000}
```

### 2. Recommended Phoenix Controller Download (Staged Export)

Use `BeamExport.Plug.send_staged_export/4` to protect your database connections:

```elixir
defmodule MyAppWeb.UserController do
  use MyAppWeb, :controller

  def export_csv(conn, _params) do
    query = from(u in MyApp.Accounts.User, order_by: u.id)

    BeamExport.Plug.send_staged_export(
      conn,
      "users_export.csv",
      {MyApp.Repo, query},
      headers: ["User ID", "Name", "Email"],
      mapper: fn u -> [u.id, u.name, u.email] end
    )
  end
end
```
*The database transaction runs in seconds and frees the connection. The file is served via `sendfile` and deleted automatically after download.*

### 3. Stream from Any Enumerable Collection

`beam_export` works with any Elixir `Enumerable`, lists, or streams:

```elixir
stream = BeamExport.to_stream(
  [%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}],
  headers: [:id, :name]
)

# Stream chunks to standard output, S3 uploader, or custom consumer:
stream
|> Stream.into(File.stream!("users.csv"))
|> Stream.run()
```

### 4. Streaming Gzip Compression on the Fly

Pass a filename ending with `.gz` or `compress: :gzip` to stream compressed `.csv.gz` files with zero external dependencies:

```elixir
# File export:
{:ok, meta} = BeamExport.to_file(
  {MyApp.Repo, query},
  "exports/users.csv.gz",
  headers: ["ID", "Name", "Email"],
  mapper: fn u -> [u.id, u.name, u.email] end
)

# Phoenix download (automatically sends Content-Type: application/gzip):
BeamExport.Plug.send_staged_export(conn, "users.csv.gz", {MyApp.Repo, query},
  headers: ["ID", "Name", "Email"],
  mapper: fn u -> [u.id, u.name, u.email] end
)
```

---

## ⚡ Performance & Benchmarks

Empirical testing on Linux (Erlang/OTP 26, Elixir 1.17) comparing traditional in-memory exports vs `BeamExport`:

| Dataset Size | Approach | Peak Process Memory | Wall-Clock Duration | Memory Advantage |
|---|---|---|---|---|
| **10,000 rows** | Traditional (`Repo.all`) | 6.94 MB | 120 ms | Baseline |
| **10,000 rows** | **`BeamExport`** | **1.06 MB** | **88 ms** | **6.5x lower** |
| | | | | |
| **50,000 rows** | Traditional (`Repo.all`) | 31.29 MB | 390 ms | Baseline |
| **50,000 rows** | **`BeamExport`** | **1.06 MB** | **276 ms** | **29.5x lower** |
| | | | | |
| **100,000 rows** | Traditional (`Repo.all`) | 53.58 MB | 747 ms | Baseline |
| **100,000 rows** | **`BeamExport`** | **1.06 MB** | **758 ms** | **50.5x lower** |

*Notice:* As dataset size expands, `Repo.all` memory scales linearly ($O(N)$), while `BeamExport` maintains a **strictly flat $O(1)$ memory footprint of ~1.06 MB**.

To run benchmarks locally:
```bash
MIX_ENV=test mix run bench/export_bench.exs
```

---

## 📡 Observability & Telemetry

`beam_export` automatically emits the following `:telemetry` events:

| Event Name | Measurements | Metadata |
|---|---|---|
| `[:beam_export, :export, :start]` | `%{system_time: integer()}` | `%{destination: atom(), format: :csv}` |
| `[:beam_export, :export, :chunk]` | `%{rows: integer(), bytes: integer()}` | `%{destination: atom()}` |
| `[:beam_export, :export, :stop]` | `%{duration: integer(), rows: integer(), bytes: integer()}` | `%{destination: atom(), format: :csv}` |
| `[:beam_export, :export, :exception]`| `%{duration: integer()}` | `%{destination: atom(), kind: :error, reason: term(), stacktrace: list()}` |

> 🔒 **Privacy Guarantee:** Telemetry payloads never contain row contents or customer data. In the event of a mapping error, the record is automatically redacted (`record: :redacted`) before emitting exception telemetry.

---

## ⚠️ Important Limitations

1. **Preloads Are Not Supported:** Relational database cursors (`Repo.stream/2`) cannot preload associations. Attempting to stream a query with `preload: [...]` will raise `BeamExport.PreloadError`. Use SQL joins (`join: assoc(u, :company)`) or select required fields directly.
2. **When NOT to use Direct Chunked Streaming:** Direct HTTP chunking (`BeamExport.Plug.send_chunked_export/4`) should only be used in low-latency internal microservices. For public-facing web applications, always use `BeamExport.Plug.send_staged_export/4` to prevent slow clients from exhausting your database connection pool.

---

## 🤝 Contributing

Contributions are warmly welcomed!

1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/my-enhancement`).
3. Ensure all tests pass: `mix test`.
4. Run formatting: `mix format`.
5. Submit a pull request.

---

## 📄 License

Licensed under the **Apache License, Version 2.0**. See [LICENSE](LICENSE) for details.
