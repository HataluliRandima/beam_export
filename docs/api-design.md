# Public API Design Specification

**Status:** Approved  
**Author:** Principal Elixir Engineering Team  
**Date:** November 2026  
**Library:** `beam_export`  

---

## 1. Design Philosophy

The public API of `beam_export` is designed according to the following core tenets:

1. **Idiomatic Elixir:** Leverages standard Elixir data structures, pipelines (`|>`), tuples, and the `Stream` abstraction.
2. **Predictable Types & Returns:** All execution functions return explicit `{:ok, metadata}` or `{:error, reason}` tuples.
3. **Composability:** Provides both a low-level lazy stream constructor (`to_stream/2`) and high-level, production-safe terminal sinks (`to_file/3`, `Plug` helpers).
4. **No Over-Engineering:** Avoids premature behaviours or excessive dependency inversion. Components are simple modules with clean functional boundaries.
5. **Safe Defaults:** Defaults to RFC 4180 compliance, memory-bounded batching (500 rows/batch), and staged file downloads over HTTP to protect database connection pools.

---

## 2. API Options Evaluated

### Option 1: Builder Pattern via Chained Pipeline
```elixir
BeamExport.source({Repo, query})
|> BeamExport.headers(["ID", "Name", "Email"])
|> BeamExport.map(&format_user/1)
|> BeamExport.to_file("users.csv")
```
* **Critique:** Highly readable and expressive. However, chaining 4–5 steps when most exports only need headers and a mapper introduces unnecessary intermediary struct manipulation when a simple options keyword list or struct is more concise.

### Option 2: Monolithic Keyword Function
```elixir
BeamExport.export(
  source: {Repo, query},
  headers: ["ID", "Name"],
  mapper: &format_user/1,
  destination: {:file, "users.csv"}
)
```
* **Critique:** Simple, but clumsy when integrating with web controllers or custom stream consumers.

### Option 3: Two-Tier API (Stream Constructor + Terminal Sinks) — **SELECTED**
A hybrid approach that provides:
1. A **Composable Stream Constructor** (`BeamExport.to_stream/2`) returning a lazy Elixir `Enumerable`/`Stream` of `iodata` chunks.
2. **Turnkey Production Sinks** for common destinations (`to_file/3`, `Plug.send_staged_export/4`, `Plug.send_chunked_export/4`).

```elixir
# 1. Direct file export (CLI, Oban jobs, local dumps)
{:ok, meta} = BeamExport.to_file({MyApp.Repo, query}, "exports/users.csv",
  headers: ["ID", "Name", "Email"],
  mapper: fn user -> [user.id, user.name, user.email] end
)

# 2. Lazy Stream extraction (S3 uploads, custom consumers)
stream = BeamExport.to_stream({MyApp.Repo, query},
  headers: ["ID", "Name", "Email"],
  mapper: fn user -> [user.id, user.name, user.email] end
)

# 3. Web Download via Staged Temp File (Recommended for Phoenix/Plug)
conn = BeamExport.Plug.send_staged_export(conn, "users.csv", {MyApp.Repo, query},
  headers: ["ID", "Name", "Email"],
  mapper: fn user -> [user.id, user.name, user.email] end
)

# 4. Web Download via Direct HTTP Chunking (Opt-in for low-latency internal APIs)
conn = BeamExport.Plug.send_chunked_export(conn, "users.csv", {MyApp.Repo, query},
  headers: ["ID", "Name", "Email"],
  mapper: fn user -> [user.id, user.name, user.email] end
)
```

---

## 3. Detailed Specification of Public Functions

### 3.1 `BeamExport.to_file/3`

Exports any enumerable source or Ecto query directly to a file on disk.

```elixir
@spec to_file(
  source :: Enumerable.t() | {module(), Ecto.Queryable.t()},
  file_path :: Path.t(),
  opts :: keyword()
) :: {:ok, %{rows: non_neg_integer(), bytes: non_neg_integer()}} | {:error, term()}
```

#### Options (`opts`):
* `:headers` (`[String.t() | atom()]`) — Optional. List of column headers prepended to output.
* `:mapper` (`(term() -> [term()])`) — Optional. Function that transforms each record into a row list. Defaults to identity `&Function.identity/1`.
* `:separator` (`String.t()` or `char()`) — Delimiter. Defaults to `","`.
* `:line_terminator` (`String.t()`) — Line ending. Defaults to `"\r\n"` (RFC 4180).
* `:max_rows` (`pos_integer()`) — Batch size for database cursor fetching. Defaults to `500`.
* `:timeout` (`timeout()`) — Database transaction timeout. Defaults to `:infinity`.
* `:sanitize_formulas` (`boolean()`) — If `true`, prepends a single quote `'` to fields starting with `=, +, -, @, \t, \r` to protect against CSV formula injection. Defaults to `false`.

#### Example:
```elixir
query = from(u in User, where: u.active == true, select: u)

case BeamExport.to_file({MyApp.Repo, query}, "/tmp/active_users.csv",
  headers: ["User ID", "Full Name", "Email Address"],
  mapper: fn u -> [u.id, u.name, u.email] end
) do
  {:ok, %{rows: rows, bytes: bytes}} ->
    Logger.info("Exported #{rows} rows (#{bytes} bytes)")

  {:error, reason} ->
    Logger.error("Export failed: #{inspect(reason)}")
end
```

---

### 3.2 `BeamExport.to_stream/2`

Creates a lazy Elixir `Stream` emitting CSV `iodata` chunks.

```elixir
@spec to_stream(
  source :: Enumerable.t() | {module(), Ecto.Queryable.t()},
  opts :: keyword()
) :: Enumerable.t()
```

* When `source` is `{Repo, query}`, the returned stream encapsulates the `Repo.transaction/2` lifecycle using `Stream.resource/3`.
* As chunks are pulled, the cursor fetches batches, transforms rows via `:mapper`, and yields encoded CSV iodata chunks.
* When the stream finishes or is halted early, the transaction is closed deterministically.

#### Example:
```elixir
# Streaming directly to an external process or collector
BeamExport.to_stream(users_list, headers: ["ID", "Name"])
|> Stream.into(File.stream!("users.csv"))
|> Stream.run()
```

---

### 3.3 `BeamExport.to_iodata/2`

Convenience function primarily for testing, scripts, and small datasets. Returns the full encoded payload as `iodata`.

```elixir
@spec to_iodata(
  source :: Enumerable.t() | {module(), Ecto.Queryable.t()},
  opts :: keyword()
) :: {:ok, iodata(), %{rows: non_neg_integer(), bytes: non_neg_integer()}} | {:error, term()}
```

---

### 3.4 `BeamExport.Plug.send_staged_export/4` (Recommended Web Download)

Executes a two-phase export:
1. Dumps the dataset into an ephemeral temporary file at database speed.
2. Closes the database transaction immediately.
3. Sets `Content-Type: text/csv` and `Content-Disposition: attachment; filename="..."`.
4. Streams the file to the client using `Plug.Conn.send_file/5`.
5. Automatically deletes the temporary file upon completion or client disconnect using `Plug.Conn.register_before_send/2`.

```elixir
@spec send_staged_export(
  conn :: Plug.Conn.t(),
  filename :: String.t(),
  source :: Enumerable.t() | {module(), Ecto.Queryable.t()},
  opts :: keyword()
) :: Plug.Conn.t()
```

#### Additional Options for Staged Export:
* `:tmp_dir` (`Path.t()`) — Temporary directory for the staged file. Defaults to `System.tmp_dir!()`.

---

### 3.5 `BeamExport.Plug.send_chunked_export/4` (Direct HTTP Chunking)

Provides direct database-to-HTTP chunking for internal systems where latency-to-first-byte is critical and connection pool starvation is not a risk.

```elixir
@spec send_chunked_export(
  conn :: Plug.Conn.t(),
  filename :: String.t(),
  source :: Enumerable.t() | {module(), Ecto.Queryable.t()},
  opts :: keyword()
) :: Plug.Conn.t()
```

* Automatically initiates chunked response: `conn |> send_chunked(200)`.
* Emits chunks in an interruptible `Enum.reduce_while/3` loop.
* Halts and terminates database cursor immediately if `chunk(conn, data)` returns `{:error, :closed}`.

---

## 4. Value Formatting Protocol / Sanitization

The mapper converts individual fields into strings or iodata. `BeamExport` handles common Elixir data types automatically:

| Input Type | Serialization Output |
|---|---|
| `nil` | `""` (empty field) |
| `binary()` | Escaped string (quotes doubled if contains comma, quote, or newline) |
| `integer()` | `Integer.to_string/1` |
| `float()` | `:erlang.float_to_binary(f, [:compact, {:decimals, 10}])` |
| `Decimal.t()` | `Decimal.to_string/1` |
| `Date.t()` | `Date.to_iso8601/1` |
| `Time.t()` | `Time.to_iso8601/1` |
| `DateTime.t()` | `DateTime.to_iso8601/1` |
| `NaiveDateTime.t()` | `NaiveDateTime.to_iso8601/1` |
| `boolean()` | `"true"` or `"false"` |
| `atom()` | `Atom.to_string/1` |

---

## 5. Error Handling Contract

Export functions adhere to strict return contracts:

* **File Destination Failures:** If a target path cannot be written to (e.g. `{:error, :eacces}` or `{:error, :enospc}`), `to_file/3` returns `{:error, {:file_error, posix_reason}}`.
* **Database Failures:** If the Ecto query fails, or transaction times out, the error is unwrapped and returned as `{:error, {:ecto_error, reason}}`.
* **Mapper Exceptions:** If a user-supplied mapper function raises an exception, the pipeline catches it, triggers resource cleanup (rolling back transaction, deleting temp file), and raises a descriptive `BeamExport.MappingError` with record context.
