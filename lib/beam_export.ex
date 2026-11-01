defmodule BeamExport do
  @moduledoc """
  A production-grade, memory-bounded streaming data export engine for Elixir.

  ## Core Features
  * **Memory Bounded:** Pure lazy streaming using `Stream` and nested `iodata`.
  * **Database Protection:** Staged temporary file exports release database connections
    in seconds, preventing pool starvation from slow clients.
  * **RFC 4180 Compliant:** Fast, zero-copy CSV encoding with quoting, newline handling,
    and optional formula injection protection.
  * **Source Agnostic:** Supports any `Enumerable`, lazy `Stream`, or `{Repo, query}` tuple.
  * **Turnkey Observability:** Standard `:telemetry` events for rows, bytes, and timing.

  ## Examples

      # Export an Ecto query to a CSV file:
      {:ok, meta} = BeamExport.to_file(
        {MyApp.Repo, query},
        "exports/users.csv",
        headers: ["ID", "Name", "Email"],
        mapper: fn user -> [user.id, user.name, user.email] end
      )

      # Build a lazy stream:
      stream = BeamExport.to_stream(users, headers: ["ID", "Name"])
  """

  alias BeamExport.Stream, as: ExportStream
  alias BeamExport.Telemetry

  @doc """
  Constructs a lazy Elixir `Stream` emitting CSV `iodata` chunks.
  """
  @spec to_stream(Enumerable.t() | {module(), Ecto.Queryable.t()}, keyword()) :: Enumerable.t()
  def to_stream(source, opts \\ []) do
    ExportStream.build(source, opts)
  end

  @doc """
  Streams data directly into a file on disk.

  Returns `{:ok, %{rows: non_neg_integer(), bytes: non_neg_integer(), path: Path.t()}}`
  or `{:error, term()}`.
  """
  @spec to_file(
          Enumerable.t() | {module(), Ecto.Queryable.t()},
          Path.t(),
          keyword()
        ) :: {:ok, map()} | {:error, term()}
  def to_file(source, path, opts \\ []) do
    BeamExport.Destination.File.write(source, path, opts)
  end

  @doc """
  Collects the export into an in-memory `iodata` payload.

  Recommended for testing, small datasets, or email attachments.

  Returns `{:ok, iodata(), %{rows: non_neg_integer(), bytes: non_neg_integer()}}`
  or `{:error, term()}`.
  """
  @spec to_iodata(
          Enumerable.t() | {module(), Ecto.Queryable.t()},
          keyword()
        ) :: {:ok, iodata(), map()} | {:error, term()}
  def to_iodata(source, opts \\ []) do
    start_time = System.monotonic_time()
    Telemetry.start(:iodata, opts)

    try do
      stream_with_counts = ExportStream.build_with_counts(source, opts)

      {reversed_chunks, total_rows, total_bytes} =
        Enum.reduce(stream_with_counts, {[], 0, 0}, fn {:chunk, rows, chunk},
                                                       {acc, r_acc, b_acc} ->
          bytes = IO.iodata_length(chunk)
          {[chunk | acc], r_acc + rows, b_acc + bytes}
        end)

      iodata = Enum.reverse(reversed_chunks)

      meta = %{
        rows: total_rows,
        bytes: total_bytes,
        duration: System.monotonic_time() - start_time
      }

      Telemetry.stop(:iodata, meta, opts)
      {:ok, iodata, meta}
    rescue
      e ->
        Telemetry.exception(:iodata, start_time, e, __STACKTRACE__, opts)
        reraise e, __STACKTRACE__
    end
  end
end
