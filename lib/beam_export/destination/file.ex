defmodule BeamExport.Destination.File do
  @moduledoc """
  Streams export data directly into files with buffered I/O, atomic file writing,
  and telemetry instrumentation.
  """

  alias BeamExport.Stream, as: ExportStream
  alias BeamExport.Telemetry

  @doc """
  Streams data from `source` directly into a file at `target_path`.

  ## Options
    * `:atomic` - when `true`, streams into a temporary file in the target directory
      and atomically renames it to `target_path` upon successful completion.
      Defaults to `false`.
    * `:headers` - list of headers to prepend.
    * `:mapper` - mapper function.
    * `:separator` - CSV delimiter. Defaults to `","`.
    * `:line_terminator` - line endings. Defaults to `"\\r\\n"`.
    * `:sanitize_formulas` - boolean to sanitize Excel formulas. Defaults to `false`.
    * `:max_rows` - Ecto batch size. Defaults to `500`.
    * `:timeout` - Ecto transaction timeout. Defaults to `:infinity`.

  Returns `{:ok, %{path: String.t(), rows: non_neg_integer(), bytes: non_neg_integer(), duration: integer()}}`
  or `{:error, term()}`.
  """
  @spec write(Enumerable.t() | {module(), Ecto.Queryable.t()}, Path.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def write(source, target_path, opts \\ []) do
    start_time = System.monotonic_time()
    Telemetry.start(:file, opts)

    target_dir = Path.dirname(target_path)
    atomic? = Keyword.get(opts, :atomic, false)

    write_path =
      if atomic? do
        random_suffix = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
        Path.join(target_dir, ".tmp_#{Path.basename(target_path)}_#{random_suffix}")
      else
        target_path
      end

    try do
      File.mkdir_p!(target_dir)

      # Build the lazy stream
      stream = ExportStream.build(source, opts)

      # Open file with delayed_write buffer for peak write throughput
      file = File.open!(write_path, [:write, :binary, :delayed_write])

      total_bytes =
        try do
          Enum.reduce(stream, 0, fn chunk, bytes_acc ->
            :ok = IO.binwrite(file, chunk)
            bytes = IO.iodata_length(chunk)
            Telemetry.chunk(:file, 0, bytes, opts)
            bytes_acc + bytes
          end)
        after
          File.close(file)
        end

      # If atomic writing was requested, atomically rename the temp file to the final path
      if atomic? do
        File.rename!(write_path, target_path)
      end

      # Determine row count:
      headers = Keyword.get(opts, :headers)
      header_offset = if is_list(headers) and headers != [], do: 1, else: 0

      total_rows =
        cond do
          is_list(source) ->
            length(source) + header_offset

          is_struct(source, Range) ->
            Range.size(source) + header_offset

          true ->
            count_lines_in_file(target_path)
        end

      meta = %{
        path: target_path,
        rows: total_rows,
        bytes: total_bytes,
        duration: System.monotonic_time() - start_time
      }

      Telemetry.stop(:file, meta, opts)
      {:ok, meta}
    rescue
      e in [File.Error, BeamExport.MappingError, BeamExport.PreloadError] ->
        cleanup_temp_file(write_path, atomic?)
        Telemetry.exception(:file, start_time, e, __STACKTRACE__, opts)
        {:error, e}

      e ->
        cleanup_temp_file(write_path, atomic?)
        Telemetry.exception(:file, start_time, e, __STACKTRACE__, opts)
        {:error, e}
    end
  end

  defp cleanup_temp_file(path, true) do
    if File.exists?(path), do: File.rm(path)
  end

  defp cleanup_temp_file(_path, _atomic), do: :ok

  defp count_lines_in_file(path) do
    path
    |> Elixir.File.stream!([], 64 * 1024)
    |> Stream.map(fn chunk ->
      chunk
      |> :binary.matches(["\n"])
      |> length()
    end)
    |> Enum.sum()
  end
end
