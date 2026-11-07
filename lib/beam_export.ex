defmodule BeamExport do
  @moduledoc """
  Memory-bounded CSV exports from Ecto queries and enumerables.

  BeamExport streams records through a lazy pipeline (`source -> rows -> CSV chunks`)
  so memory stays proportional to `:chunk_size` rather than to the size of the export.

    * `to_file/3` writes a file atomically (temporary file, sync, rename).
    * `to_iodata/2` collects a small export in memory.
    * `to_stream/2` returns a lazy stream of CSV `iodata` chunks.
    * `BeamExport.Plug` serves downloads from Plug/Phoenix.

  ## Sources

    * any `Enumerable` (lists, ranges, streams)
    * `{Repo, queryable}`: an Ecto query or schema. Preloads are not supported
      (`BeamExport.PreloadError`); use joins or `select` instead.

  ## Records

  Each record becomes one CSV row:

    * with `:mapper`, the mapper's return value: a list (or tuple) of field values
    * a list or tuple is used as-is (so Ecto `select: [u.id, u.name]` and
      `select: {u.id, u.name}` work without a mapper)
    * a map or struct is read by `:headers`. Headers must be its keys: atoms, or
      strings naming existing atom keys (`:id` or `"id"`). A header that is not a field
      of a struct, or a map row that matches none of the headers, raises
      `BeamExport.EncodeError` rather than exporting blank columns. To use
      human-readable titles such as `"Email Address"`, pass a `:mapper`.

  ## Values

  | Value | CSV text |
  |---|---|
  | `nil` | empty field |
  | strings, atoms | as-is (formula-sanitized, see below) |
  | integers | decimal digits |
  | floats | shortest exact form: `0.1`, `1.0e-12` (`Float.to_string/1`) |
  | `Decimal` | plain notation, never scientific: `1000`, `-0.00012` |
  | `Date`, `Time`, `NaiveDateTime`, `DateTime` | ISO 8601 |
  | booleans | `true` / `false` |
  | other structs implementing `String.Chars` | `to_string/1` |

  Lists, maps, tuples and other terms raise `BeamExport.EncodeError`: convert them
  explicitly in your `:mapper` (for example with `Enum.join/2` or a JSON encoder).

  ## Options

  Unknown options raise `ArgumentError`, so a typo cannot silently disable a feature.

  Common to every function:

    * `:headers` - list of column titles (strings or atoms) written as the first row.
      Also selects the keys read from map/struct records when no `:mapper` is given.
    * `:mapper` - `fn record -> [field, ...] end`.
    * `:chunk_size` - rows per emitted chunk (and per file write). Default `500`.
    * `:separator` - field separator. Default `","`. Must not contain `"`, CR or LF.
    * `:line_terminator` - `"\\r\\n"` (default, RFC 4180) or `"\\n"`.
    * `:sanitize_formulas` - neutralize spreadsheet formula injection. Default `true`.
      See "Security".
    * `:include_record_in_errors` - include record contents in `BeamExport.MappingError`.
      Default `false`. Never affects telemetry, which is always redacted.

  Ecto sources:

    * `:max_rows` - rows fetched from the database per round trip. Default `500`.
    * `:timeout` - how long the export may hold its database connection, in
      milliseconds, or `:infinity` (default). This bounds the *whole* export: when it
      elapses, the export fails.
    * `:consumer_timeout` - `to_stream/2` only, outside a transaction: how long the
      database worker waits for the consumer to ask for the next batch. Default
      `30_000`. When it elapses the cursor is closed and the consumer gets
      `BeamExport.TimeoutError`.
    * `:batch_timeout` - `to_stream/2` only, outside a transaction: how long the
      consumer waits for the database to deliver the next batch. Default `120_000`.

  `to_file/3` only:

    * `:atomic` - write to a temporary file in the target directory, sync it, then
      rename it over the target. Default `true`. With `atomic: false` the target is
      written in place: a failure after the target was opened leaves no partial
      file, but the previous contents are lost.
    * `:compress` - `true` or `:gzip` to gzip the output; `false` to never compress.
      By default, compression is enabled when the path ends in `.gz`.
    * `:file_mode` - permissions of the file created by the export. Default `0o600`
      (owner read/write only). An existing file written with `atomic: false` keeps
      its permissions.

  ## Return values and errors

  `to_file/3` and `to_iodata/2` return `{:ok, ...}` or `{:error, exception}`. Every
  error is an exception struct:

    * `BeamExport.MappingError` - the mapper raised or returned a non-list.
    * `BeamExport.EncodeError` - a record or value has no faithful CSV form.
    * `BeamExport.PreloadError` - the query has preloads.
    * `BeamExport.TimeoutError` - an Ecto stream stopped making progress.
    * `BeamExport.ExportError` - other failures, such as a transaction that did not commit.
    * `File.Error` / `File.RenameError` - filesystem errors, including errors only
      reported when the file is closed (for example a full disk).
    * exceptions raised by your source or database driver, unchanged.

  Invalid options raise `ArgumentError`. Exits and throws from the source are not
  converted into error tuples; `to_file/3` still removes its partial file.

  `to_stream/2` is lazy: the same exceptions are *raised* while the stream is consumed.

  An export that does not complete is never reported as successful.

  ## Security

    * **Formula injection**: by default, text fields starting with `=`, `+`, `-`, `@`,
      TAB or CR (optionally after whitespace) are prefixed with `'` so spreadsheet
      applications display rather than evaluate them (OWASP recommendation). This
      changes such values, for example `"-5"` (a string) becomes `"'-5"`. Numbers are
      never changed. Pass `sanitize_formulas: false` for machine-to-machine exports.
    * **Errors and telemetry** never contain record contents unless you pass
      `include_record_in_errors: true` (which still never applies to telemetry).
      Exceptions raised by your own source or database driver are passed through.
    * **Files** are created with mode `0o600`. Never pass untrusted input as the
      `path` of `to_file/3` or the `:tmp_dir` of `BeamExport.Plug`.
    * **Resources**: BeamExport imposes no limit on export size or concurrency.
      Rate-limit export endpoints, and run large exports in background jobs.

  ## Examples

      import Ecto.Query

      query = from(u in MyApp.User, where: u.active, order_by: u.id)

      {:ok, %{rows: rows, bytes: bytes}} =
        BeamExport.to_file({MyApp.Repo, query}, "/var/exports/users.csv",
          headers: ["ID", "Name", "Email"],
          mapper: fn user -> [user.id, user.name, user.email] end
        )

      # Map records with key headers, no mapper needed:
      {:ok, iodata, _meta} =
        BeamExport.to_iodata([%{id: 1, name: "Alice"}], headers: [:id, :name])
  """

  alias BeamExport.Options
  alias BeamExport.Stream, as: ExportStream
  alias BeamExport.Telemetry

  @doc """
  Returns a lazy stream of CSV `iodata` chunks (one per `:chunk_size` rows, plus
  one for the header row).

  Nothing runs until the stream is consumed. Errors are raised during consumption.

  For an Ecto source outside a transaction, a dedicated worker process holds the
  database transaction and cursor while the stream is consumed. Consume it promptly:
  if the consumer does not ask for the next batch within `:consumer_timeout`, the
  cursor is closed and `BeamExport.TimeoutError` is raised. The export is then
  incomplete, and BeamExport never reports it as complete. Inside a transaction
  (`Repo.transaction/2`), the caller's connection is used directly.

  This function emits no telemetry events.

  See the module documentation for options.
  """
  @spec to_stream(Enumerable.t() | {module(), Ecto.Queryable.t()}, keyword()) :: Enumerable.t()
  def to_stream(source, opts \\ []) do
    opts = Options.validate!(opts, :to_stream)
    ExportStream.build(source, opts)
  end

  @doc """
  Writes the export to the file at `path`, creating parent directories.

  Returns `{:ok, meta}` where `meta` contains:

    * `:path` - the target path
    * `:rows` - rows written, **including the header row**
    * `:bytes` - bytes written to disk (compressed size when gzipped)
    * `:compressed` - whether the file is gzip-compressed
    * `:duration` - in `:native` time units (`System.convert_time_unit/3`)

  Returns `{:error, exception}` on failure (see "Return values and errors").

  Ecto sources run in a transaction owned by the calling process (or the caller's
  existing transaction), which holds one database connection until the file is
  written.

  By default the write is atomic: a failed export leaves an existing file at `path`
  untouched and never leaves partial or temporary files behind. The file is synced
  to disk before the rename; the parent directory is not (there is no portable way to
  do so), so on some filesystems a power loss right after the rename can leave the
  previous file in place.

  See the module documentation for options.
  """
  @spec to_file(
          Enumerable.t() | {module(), Ecto.Queryable.t()},
          Path.t(),
          keyword()
        ) :: {:ok, map()} | {:error, Exception.t()}
  def to_file(source, path, opts \\ []) do
    BeamExport.Destination.File.write(source, path, opts)
  end

  @doc """
  Collects the whole export in memory as `iodata`.

  Memory grows with the size of the export: use it for small exports (tests, email
  attachments). Use `to_file/3` or `to_stream/2` for large ones.

  Returns `{:ok, iodata, %{rows: rows, bytes: bytes, duration: duration}}`, where
  `rows` includes the header row and `duration` is in `:native` time units, or
  `{:error, exception}` (see "Return values and errors").

  Ecto sources run in a transaction owned by the calling process.

  See the module documentation for options.
  """
  @spec to_iodata(
          Enumerable.t() | {module(), Ecto.Queryable.t()},
          keyword()
        ) :: {:ok, iodata(), map()} | {:error, Exception.t()}
  def to_iodata(source, opts \\ []) do
    opts = Options.validate!(opts, :to_iodata)
    # Invalid sources are programmer errors: raise, like invalid options.
    _ = BeamExport.Ecto.ecto_source?(source)
    start_time = System.monotonic_time()
    Telemetry.start(:iodata, opts)

    result =
      try do
        BeamExport.Ecto.run(source, opts, fn -> collect_iodata(source, opts) end)
      rescue
        e -> {:error, e, __STACKTRACE__}
      end

    case result do
      {:ok, {iodata, rows, bytes}} ->
        meta = %{rows: rows, bytes: bytes, duration: System.monotonic_time() - start_time}
        Telemetry.stop(:iodata, meta, opts)
        {:ok, iodata, meta}

      {:error, exception, stacktrace} ->
        Telemetry.exception(:iodata, start_time, exception, stacktrace, opts)
        {:error, exception}
    end
  end

  defp collect_iodata(source, opts) do
    {reversed_chunks, total_rows, total_bytes} =
      source
      |> ExportStream.build_with_counts(opts)
      |> Enum.reduce({[], 0, 0}, fn {:chunk, rows, chunk}, {acc, r_acc, b_acc} ->
        {[chunk | acc], r_acc + rows, b_acc + IO.iodata_length(chunk)}
      end)

    {:ok, {Enum.reverse(reversed_chunks), total_rows, total_bytes}}
  rescue
    e -> {:error, e, __STACKTRACE__}
  end
end
